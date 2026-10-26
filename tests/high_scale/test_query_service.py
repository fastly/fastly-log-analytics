from __future__ import annotations

from datetime import UTC, datetime

import pytest
from backend.high_scale.archive_models import ServingWatermark

from backend.high_scale._common import KeysetCursor
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts


class _Client:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params
        return self.rows


def _watermark() -> ServingWatermark:
    return ServingWatermark(
        "svc",
        "request",
        1,
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )


def _row(second: int, event_id: str) -> dict:
    return {"event_id": event_id, "timestamp": datetime(2026, 9, 11, 20, 0, second, tzinfo=UTC)}


def test_request_facts_use_visibility_fence_and_keyset_cursor() -> None:
    client = _Client([_row(1, "event-1"), _row(2, "event-2")])
    page = query_request_facts(
        client,
        service_id="svc",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=_watermark(),
        limit=1,
        now=datetime(2026, 9, 11, 20, 6, tzinfo=UTC),
    )

    assert "publication_state = 'visible'" in client.sql
    assert "high_scale_batch_publications FINAL" in client.sql
    assert page.rows == (client.rows[0],)
    assert page.next_cursor
    decoded = KeysetCursor.decode(page.next_cursor, b"secret")
    assert decoded.event_id == "event-1"
    assert page.metadata.exact is True
    assert page.metadata.freshness_lag_seconds == 60


def test_request_facts_reject_cursor_for_other_domain() -> None:
    cursor = KeysetCursor(
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        "event-1",
        "svc",
        "rum_vitals",
    ).encode(b"secret")

    with pytest.raises(ValueError, match="service and domain"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
            cursor=cursor,
        )


def test_request_facts_reject_invalid_window_and_limit() -> None:
    with pytest.raises(ValueError, match="query end"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
        )
    with pytest.raises(ValueError, match="between"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
            limit=501,
        )


def test_rum_domains_use_separate_fact_tables_and_watermarks() -> None:
    client = _Client([_row(1, "event-1")])
    watermark = _watermark()
    watermark = ServingWatermark(
        watermark.service_id,
        "rum_vitals",
        watermark.owner_epoch,
        watermark.coverage_start,
        watermark.coverage_end,
        watermark.last_accepted_cursor,
        watermark.last_archived_event_id,
        watermark.last_visible_event_id,
        watermark.exact,
    )

    query_rum_facts(
        client,
        service_id="svc",
        domain="rum_vitals",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=watermark,
    )

    assert "FROM rum_vitals_facts" in client.sql


def test_cmcd_facts_use_projection_identity_for_keyset() -> None:
    client = _Client([_row(1, "projection-1")])
    watermark = _watermark()
    watermark = ServingWatermark(
        watermark.service_id,
        "cmcd",
        watermark.owner_epoch,
        watermark.coverage_start,
        watermark.coverage_end,
        watermark.last_accepted_cursor,
        watermark.last_archived_event_id,
        watermark.last_visible_event_id,
        watermark.exact,
    )

    query_cmcd_facts(
        client,
        service_id="svc",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=watermark,
    )

    assert "FROM cmcd_projection_facts" in client.sql
    assert "projection_key AS event_id" in client.sql


# ── Merged from test_query_router.py ──

from datetime import timedelta

from backend.high_scale.query_router import (
    QueryIntent,
    QueryRequest,
    QueryRouter,
    QueryRouterConfig,
    QueryTier,
)

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def request(*, hours_old: int, hours: int = 1, intent: QueryIntent = QueryIntent.RAW) -> QueryRequest:
    end = NOW - timedelta(hours=hours_old)
    return QueryRequest("svc", "request", end - timedelta(hours=hours), end, intent)


def test_recent_triage_uses_aggregates() -> None:
    plan = QueryRouter().plan(request(hours_old=2, intent=QueryIntent.TRIAGE), now=NOW)

    assert plan.tier is QueryTier.TRIAGE_AGGREGATE
    assert plan.sources == ("aggregates",)
    assert not plan.queued


def test_recent_raw_uses_clickhouse_facts() -> None:
    plan = QueryRouter().plan(request(hours_old=2), now=NOW)

    assert plan.tier is QueryTier.RECENT_FACTS
    assert plan.sources == ("clickhouse_facts",)
    assert plan.bounded is False


def test_warm_history_reads_facts_and_archive() -> None:
    plan = QueryRouter().plan(request(hours_old=24 * 10), now=NOW)

    assert plan.tier is QueryTier.WARM_FACTS_ARCHIVE
    assert plan.sources == ("clickhouse_facts", "fos_archive")


def test_cold_history_is_queued_and_bounded() -> None:
    plan = QueryRouter().plan(request(hours_old=24 * 45, hours=2), now=NOW)

    assert plan.tier is QueryTier.COLD_JOB
    assert plan.queued
    assert plan.bounded
    assert plan.max_rows == 500


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
    ],
)
def test_query_range_and_identity_validation(request_value: QueryRequest, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        QueryRouter().plan(request_value, now=NOW)


def test_future_ranges_are_rejected() -> None:
    future = QueryRequest("svc", "request", NOW - timedelta(minutes=1), NOW + timedelta(minutes=1))

    with pytest.raises(ValueError, match="future"):
        QueryRouter().plan(future, now=NOW)


def test_invalid_router_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="warm retention"):
        QueryRouter(QueryRouterConfig(warm_retention=timedelta(hours=1)))


# ── Merged from test_request_filters.py ──

import pytest

from backend.high_scale._common import request_filter_clauses
from backend.models.common import FilterSpec


def test_include_and_exclude_become_typed_in_predicates() -> None:
    clauses, params = request_filter_clauses(
        {
            "country": FilterSpec(mode="include", values=["US", "CA"]),
            "status": {"mode": "exclude", "values": [404, "500"]},
        }
    )

    assert clauses == [
        "(country IN ({flt_0_0:String}, {flt_0_1:String}))",
        "(custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String}, {flt_1_1:String}))",
    ]
    assert params == {
        "flt_0_0": "US",
        "flt_0_1": "CA",
        "flt_k1": "status",
        "flt_1_0": "404",
        "flt_1_1": "500",
    }


def test_ip_aliases_resolve_to_the_client_ip_column() -> None:
    for key in ("ip", "client_ip", "IP", "xfilter_ip_2"):
        clauses, _ = request_filter_clauses({key: {"mode": "include", "values": ["203.0.113.7"]}})
        assert clauses == ["(client_ip IN ({flt_0_0:String}))"]


def test_hostile_keys_never_reach_the_sql_text() -> None:
    clauses, params = request_filter_clauses({"status'] OR 1=1 --": {"mode": "include", "values": ["x"]}})

    sql = " ".join(clauses)
    assert "OR 1=1" not in sql and "'" not in sql
    assert params["flt_k0"] == "statusor11"


def test_blank_values_and_empty_filters_are_skipped() -> None:
    clauses, params = request_filter_clauses(
        {"country": {"mode": "include", "values": ["", "  "]}, "pop": {"mode": "include", "values": []}}
    )

    assert clauses == [] and params == {}


def test_null_values_mean_the_map_field_is_absent() -> None:
    include, _ = request_filter_clauses({"pop": {"mode": "include", "values": [None, "SJC"]}})
    exclude, _ = request_filter_clauses({"pop": {"mode": "exclude", "values": [None]}})

    assert include == ["(custom_fields[{flt_k0:String}] IN ({flt_0_0:String}) OR custom_fields[{flt_k0:String}] = '')"]
    assert exclude == ["(custom_fields[{flt_k0:String}] != '')"]


def test_float_fields_filter_on_their_rounded_integer() -> None:
    clauses, params = request_filter_clauses({"ttl": {"mode": "include", "values": [3600]}})

    assert clauses == [
        "(toString(toInt32(round(toFloat64OrZero(custom_fields[{flt_k0:String}])))) IN ({flt_0_0:String}))"
    ]
    assert params["flt_k0"] == "ttl"


@pytest.mark.parametrize("key", ["_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind"])
def test_filters_the_fact_table_cannot_express_raise(key: str) -> None:
    with pytest.raises(ValueError, match="do not support filtering"):
        request_filter_clauses({key: {"mode": "include", "values": ["x"]}})


def test_no_filters_yield_no_clauses() -> None:
    assert request_filter_clauses(None) == ([], {})
    assert request_filter_clauses({}) == ([], {})


# ── Merged from test_high_scale_response_cache.py ──

import pytest

from backend.high_scale import dashboard, security
from backend.models.dashboard import AggregatesRequest
from tests.high_scale import test_cmcd as cmcd_cases
from tests.high_scale import test_dashboard as dashboard_cases
from tests.high_scale import test_security as security_cases


def test_high_scale_performance_cache_observations_do_not_execute_the_repeat() -> None:
    client = _PerfClient()
    first = _run_perf(client, sections={"waterfall"})
    count = len(client.calls)
    assert count > 0
    assert first.response_cache.state == "miss"
    hit = _run_perf(client, sections={"waterfall"})
    assert hit.response_cache.state == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


def test_high_scale_security_cache_observations_do_not_execute_the_repeat() -> None:
    client = security_cases._Client()
    service = security_cases._service(client)
    req = security_cases._req()
    first = security.security_aggregates(
        service, req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    count = len(client.calls)
    assert count > 0
    assert first.response_cache.state == "miss"
    hit = security.security_aggregates(
        service, req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    assert hit.response_cache.state == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


def test_high_scale_cmcd_cache_observations_do_not_execute_the_repeat() -> None:
    client = cmcd_cases._Client()
    first = cmcd_cases._run(client, sections={"overview"})
    count = len(client.calls)
    assert count > 0
    assert first["_response_cache"]["state"] == "miss"
    hit = cmcd_cases._run(client, sections={"overview"})
    assert hit["_response_cache"]["state"] == "hit"
    assert len(client.calls) == count


def test_high_scale_dashboard_cache_observations_survive_the_typed_response() -> None:
    service, client = dashboard_cases._recording_service()
    req = AggregatesRequest(fields=["url"], include_time_series=False)
    first = dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    count = len(client.calls)
    assert count > 0
    assert first.model_dump(by_alias=True)["_response_cache"]["state"] == "miss"
    hit = dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert hit.model_dump(by_alias=True)["_response_cache"]["state"] == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


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

    assert changed.response_cache.state == "miss"
    assert len(client.calls) > previous_calls
    changed_calls = len(client.calls)
    repeat = dashboard.aggregates(service, req, start, end)
    assert repeat.response_cache.state == "hit"
    assert len(client.calls) == changed_calls


# ── Merged from test_section_attribution.py ──

from types import SimpleNamespace

import pytest
from backend.core.high_scale_sections import current_clickhouse_scope


class _ScopeClient:
    def __init__(self) -> None:
        self.scopes: list[dict] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.scopes.append(current_clickhouse_scope())
        return []


@pytest.mark.parametrize("reader", ["origin", "performance", "security"])
def test_selected_reader_sections_attribute_real_dispatch_without_extra_queries(reader: str) -> None:
    from backend.high_scale import origin, performance, security

    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    req = SimpleNamespace(filters={}, bucket_seconds=3600, sort_by="p99")
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    if reader == "origin":
        result = origin.aggregates(service, req, start, end, sections={"status_codes"})
        timings = result["section_timings"]
        source = "projection"
    elif reader == "performance":
        result = performance.performance_aggregates(service, req, start, end, sections={"ttl_dist"})
        timings = result.section_timings
        source = "projection"
    else:
        result = security.security_aggregates(service, req, start, end, sections={"req_size_dist"})
        timings = result.section_timings
        source = "fact"
    assert len(client.scopes) == 1
    assert client.scopes[0]["service_id"] == "service-test"
    assert client.scopes[0]["source"] == source
    assert len(timings) == 1
    assert timings[0]["section_id"] == client.scopes[0]["section_id"]
    assert timings[0]["server_cpu_ms"] is None
    assert timings[0]["timing_kind"] == "client_section_wall"


@pytest.mark.parametrize("reader,expected_queries", [("transport", 2), ("quality", 5)])
def test_network_queries_have_named_fact_sections(reader: str, expected_queries: int) -> None:
    from backend.models.network import NetworkQualityRequest

    from backend.high_scale import network

    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    if reader == "transport":
        result = network.get_transport_metrics(service, start, end, {})
    else:
        result = network.network_quality(service, NetworkQualityRequest(), start, end)
    assert len(client.scopes) == expected_queries
    assert all(scope["service_id"] == "service-test" and scope["source"] == "fact" for scope in client.scopes)
    assert len({scope["section_id"] for scope in client.scopes}) == expected_queries
    assert len(result.section_timings) == expected_queries


def test_bot_component_reports_mixed_dispatch_while_queries_keep_concrete_sources() -> None:
    from backend.high_scale import security

    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    response = security.top_bots(service, SimpleNamespace(filters={}), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert response.section_timings[0]["source"] == "mixed"
    assert [scope["source"] for scope in client.scopes] == ["fact", "projection"]
    assert len({scope["section_id"] for scope in client.scopes}) == 1


def test_network_bundle_keeps_pop_section_identity_without_changing_list_payload() -> None:
    from backend.models.network import NetworkBundleRequest

    from backend.high_scale import network

    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    response = network.network_bundle(
        service, NetworkBundleRequest(sections=["pop_health"]), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    )
    assert response.pop_health == []
    assert len(client.scopes) == 1
    assert len(response.section_timings) == 1
    assert response.section_timings[0]["section_id"] == client.scopes[0]["section_id"]
    assert response.section_timings[0]["section"] == "network.pop_health"


# ── Merged from test_historical_jobs.py ─────────────────────────────────────

from threading import Event
from time import monotonic, sleep

from backend.high_scale.query_router import (
    QueryIntent,
    QueryPlan,
)

from backend.high_scale.historical_jobs import (
    HistoricalJob,
    HistoricalJobManager,
    HistoricalJobState,
)


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


def test_job_completes_with_a_row_and_byte_bound() -> None:
    manager = HistoricalJobManager(max_output_bytes=100, terminal_grace_ttl=timedelta(seconds=60))

    job_id = manager.submit(cold_plan(), lambda: [{"id": 1}, {"id": 2}])
    job = wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    assert job.rows == ({"id": 1}, {"id": 2})
    assert job.row_count == 2
    assert job.output_bytes <= 100

    manager.shutdown()


def test_iteration_stops_at_max_rows_without_retaining_extra_rows() -> None:
    manager = HistoricalJobManager(max_output_bytes=10_000)
    consumed: list[int] = []

    def rows() -> list[dict[str, int]]:
        return [{"id": index} for index in range(10)]

    def bounded_rows():
        for row in rows():
            consumed.append(row["id"])
            yield row

    job_id = manager.submit(cold_plan(max_rows=2), bounded_rows)
    job = wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    assert job.rows == ({"id": 0}, {"id": 1})
    assert job.row_count == 2
    assert len(consumed) <= 3
    manager.shutdown()


def test_cancellation_is_observed_during_iteration() -> None:
    manager = HistoricalJobManager(max_output_bytes=10_000)
    started = Event()
    release = Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows)
    assert started.wait(1)
    assert manager.cancel(job_id) is True
    release.set()
    job = wait_for(manager, job_id, HistoricalJobState.CANCELLED)

    assert job.rows == ({"id": 1},)
    manager.shutdown()


def test_expiry_is_observed_during_iteration() -> None:
    clock = [NOW]
    manager = HistoricalJobManager(ttl=timedelta(seconds=1), max_output_bytes=10_000, clock=lambda: clock[0])
    started = Event()
    release = Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows, now=clock[0])
    assert started.wait(1)
    clock[0] += timedelta(seconds=2)
    release.set()
    job = wait_for(manager, job_id, HistoricalJobState.EXPIRED)

    assert job.rows == ({"id": 1},)
    manager.shutdown()


def test_output_byte_limit_fails_and_drops_payload() -> None:
    manager = HistoricalJobManager(max_output_bytes=20)
    job_id = manager.submit(cold_plan(max_rows=10), lambda: [{"value": "x" * 100}])

    job = wait_for(manager, job_id, HistoricalJobState.FAILED)

    assert job.rows == ()
    assert job.error == "historical job output exceeds byte limit"
    manager.shutdown()


def test_terminal_jobs_are_purged_after_grace_ttl() -> None:
    manager = HistoricalJobManager(terminal_grace_ttl=timedelta(seconds=1))
    job_id = manager.submit(cold_plan(), lambda: [])
    wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    completed = manager.get(job_id)
    assert completed is not None and completed.terminal_at is not None

    assert manager.get(job_id, now=completed.terminal_at + timedelta(seconds=2)) is None
    manager.shutdown()


@pytest.mark.parametrize(
    "plan",
    [
        QueryPlan(
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
    ],
)
def test_only_bounded_cold_plans_can_be_submitted(plan: QueryPlan) -> None:
    manager = HistoricalJobManager()

    with pytest.raises(ValueError, match="cold"):
        manager.submit(plan, lambda: [])

    manager.shutdown()


# ── Merged from tests/high_scale/test_sessions.py ──

import re
import sqlite3
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, patch

from backend.high_scale.aggregate_query import (
    AggregateRequest,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.high_scale.performance_projection import (
    SCATTER_SAMPLE_SIZE,
    build_performance_projection_rows,
)
from backend.high_scale.recovery import RecoveryReport
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.sessions import sessions_detail, sessions_endpoint
from backend.models.network import NetworkBundleRequest, NetworkHealthRequest, NetworkQualityRequest
from backend.models.performance import PerformanceAggregatesResponse

from backend.high_scale import performance as hs_performance
from backend.high_scale import sessions as hs_sessions
from backend.high_scale.dashboard import aggregates, bundle
from backend.high_scale.network import (
    get_pop_health,
    get_transport_metrics,
    network_bundle,
    network_health,
    network_quality,
)
from backend.models.dashboard import SessionDetailRequest, SessionsRequest, SessionsResponse

_SESS_START = "2026-09-15T00:00:00Z"
_SESS_END = "2026-09-16T00:00:00Z"


def _session_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "ip": "203.0.113.7",
        "ja4": "t13d1516h2_abc",
        "session_start": datetime(2026, 9, 15, 10, 0, 0, tzinfo=UTC),
        "session_end": "2026-09-15 10:20:00.000",
        "req_count": 1500,
        "asn": 7922,
        "country": "US",
        "unique_urls": 40,
        "ua": "curl/8",
        "reqs_4xx": 10,
        "reqs_5xx": 1,
        "total_bytes": 123456,
        "median_rtt_ms": 12.5,
        "edge_count": 1000,
        "shield_count": 500,
        "edge_sid": "es-1",
        "streaming_reqs": 3,
        "flagged": 1,
        "is_streaming": 1,
        "sid": 0,
        "total_sessions": 321,
        "any_rtt": 1,
        "any_ja4": 1,
        "any_edge": 1,
        "any_edge_sid": 1,
        "any_cmcd": 1,
    }
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


def test_filters_narrow_the_events_that_are_grouped_into_sessions() -> None:
    client = _SessClient()
    _run_sess(
        client,
        filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}},
    )
    sql, params = client.sql("sessions:page")
    assert "country IN ({flt_0_0:String})" in sql
    assert "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
    assert params["flt_0_0"] == "US" and params["flt_k1"] == "pop" and params["flt_1_0"] == "SJC"


def test_events_are_fenced_to_visible_published_batches_inside_the_window() -> None:
    client = _SessClient()
    _run_sess(client)
    sql, params = client.sql("sessions:page")
    assert "publication_state='visible'" in sql
    assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
    assert "event_timestamp >= {start_time:DateTime64(3)}" in sql
    assert params["service_id"] == "svc"


def test_sorting_honours_the_requested_column_and_direction() -> None:
    client = _SessClient()
    _run_sess(client, sort_by="req_count", sort_dir="asc")
    sql, _ = client.sql("sessions:page")
    assert "ORDER BY req_count ASC" in sql


def test_unknown_sort_columns_fall_back_to_session_start() -> None:
    client = _SessClient()
    _run_sess(client, sort_by="1; DROP TABLE x")
    sql, _ = client.sql("sessions:page")
    assert "ORDER BY session_start DESC" in sql
    assert "DROP TABLE" not in sql


def test_paging_uses_typed_parameters_and_the_page_offset() -> None:
    client = _SessClient()
    _run_sess(client, page=3, limit=25)
    sql, params = client.sql("sessions:page")
    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in sql
    assert params["limit"] == 25 and params["offset"] == 50


def test_flag_thresholds_default_like_the_standard_path_and_are_reported() -> None:
    client = _SessClient()
    result = _run_sess(client)
    _, params = client.sql("sessions:page")
    assert params["min_reqs_flag"] == 1000 and params["min_4xx_pct_flag"] == 20.0
    assert result["min_reqs_flag"] == 1000 and result["min_4xx_pct_flag"] == 20.0
    custom = _run_sess(_SessClient(), min_reqs_flag=50, min_4xx_pct_flag=5.5)
    assert custom["min_reqs_flag"] == 50 and custom["min_4xx_pct_flag"] == 5.5


def test_streaming_sessions_are_detected_from_the_cmcd_map() -> None:
    client = _SessClient()
    _run_sess(client, streaming_only=True)
    sql, params = client.sql("sessions:page")
    assert "cmcd['sid']" in sql
    assert "custom_fields['cmcd_sid']" not in sql
    assert "streaming_only" in params and params["streaming_only"] == 1


def test_edge_and_shield_counts_understand_stored_boolean_spellings() -> None:
    client = _SessClient()
    _run_sess(client)
    sql, _ = client.sql("sessions:page")
    assert "lower(edge) IN ('1', 'true')" in sql
    assert "lower(edge) IN ('0', 'false')" in sql


def test_total_is_the_real_session_count_not_a_page_size_guess() -> None:
    result = _run_sess(_SessClient(), limit=1)
    assert result["total"] == 321
    assert result["page"] == 1 and result["limit"] == 1


def test_an_empty_page_past_the_end_reports_the_real_total() -> None:
    client = _SessClient(rows=[], total=40)
    result = _run_sess(client, page=9, limit=5)
    assert result["sessions"] == [] and result["total"] == 40
    assert "sessions:count" in " ".join(sql for sql, _ in client.calls)


def test_an_empty_first_page_needs_no_count_query() -> None:
    client = _SessClient(rows=[])
    result = _run_sess(client)
    assert result["total"] == 0
    assert all("sessions:count" not in sql for sql, _ in client.calls)


def test_field_availability_comes_from_the_data() -> None:
    present = _run_sess(_SessClient())
    absent = _run_sess(
        _SessClient(
            rows=[_session_row(any_rtt=0, any_ja4=0, any_edge=0, any_edge_sid=0, any_cmcd=0, median_rtt_ms=None)]
        )
    )
    assert (
        present["has_rtt"],
        present["has_ja4"],
        present["has_edge"],
        present["has_edge_sid"],
        present["has_cmcd"],
    ) == (True, True, True, True, True)
    assert (absent["has_rtt"], absent["has_ja4"], absent["has_edge"], absent["has_edge_sid"], absent["has_cmcd"]) == (
        False,
        False,
        False,
        False,
        False,
    )


def test_rows_are_clean_session_dicts_with_utc_timestamps_and_hoisted_asn_labels() -> None:
    result = _run_sess(_SessClient())
    (row,) = result["sessions"]
    assert row["session_start"] == "2026-09-15T10:00:00Z"
    assert row["session_end"] == "2026-09-15T10:20:00.000Z"
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
    assert result["asn_names"] == {7922: "AS7922 Name"}


def test_output_validates_against_the_standard_response_model() -> None:
    model = SessionsResponse.model_validate(_run_sess(_SessClient()))
    assert model.total == 321 and model.sessions[0].asn == 7922 and model.asn_names[7922] == "AS7922 Name"


def test_clickhouse_failures_propagate() -> None:
    with pytest.raises(RuntimeError, match="unavailable"):
        _run_sess(_SessClient(fail=True))


def test_detail_is_fenced_to_visible_batches_and_parameterised() -> None:
    client = _SessClient()
    client.execute = lambda sql, params=None: (  # type: ignore[method-assign]
        client.calls.append((sql, params or {})) or []
    )
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


def test_two_phase_session_enrichment_defers_heavy_states_to_phase_2() -> None:
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
                return [
                    {
                        "row_idx": 0,
                        "asn": 15169,
                        "country": "DE",
                        "unique_urls": 77,
                        "ua": "Mozilla/5.0",
                        "median_rtt_ms": 8.25,
                        "edge_sid": "es-fast",
                    }
                ]
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
    assert "sessions:enrich" in enrich_sql
    assert enrich_params["en_ip_0"] == "203.0.113.7"
    assert res["sessions"][0]["asn"] == 15169
    assert res["sessions"][0]["country"] == "DE"
    assert res["sessions"][0]["unique_urls"] == 77
    assert res["sessions"][0]["ua"] == "Mozilla/5.0"
    assert res["sessions"][0]["median_rtt_ms"] == 8.25
    assert res["sessions"][0]["edge_sid"] == "es-fast"


class PredicateClient:
    def __init__(self, timestamps: list[datetime]) -> None:
        self.calls: list[str] = []
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("toStartOfMinute", 1, lambda value: value[:16] + ":00.000000")
        self.db.executescript(
            "CREATE TABLE request_facts "
            "(event_timestamp TEXT, url TEXT, country TEXT, service_id TEXT, "
            "batch_id TEXT, publication_state TEXT);"
            "CREATE TABLE high_scale_batch_publications "
            "(service_id TEXT, batch_id TEXT, domain TEXT, publication_state TEXT);"
            "INSERT INTO high_scale_batch_publications VALUES "
            "('svc', 'facts', 'request', 'visible'), "
            "('svc', 'projection', 'request_aggregate', 'visible');"
        )
        self.db.executemany(
            "INSERT INTO request_facts VALUES (?, '/a', 'US', 'svc', 'facts', 'visible')",
            [(self.stamp(value),) for value in timestamps],
        )
        self.db.execute(
            "CREATE TABLE request_aggregates AS "
            "SELECT toStartOfMinute(event_timestamp) AS bucket_start, "
            "'url' AS dimension, url AS value, count(*) AS request_count, "
            "service_id, 'projection' AS batch_id, publication_state "
            "FROM request_facts "
            "GROUP BY bucket_start, url, service_id, publication_state "
            "UNION ALL "
            "SELECT toStartOfMinute(event_timestamp), 'country', country, count(*), "
            "service_id, 'projection', publication_state FROM request_facts "
            "GROUP BY toStartOfMinute(event_timestamp), country, "
            "service_id, publication_state"
        )

    @staticmethod
    def stamp(value: datetime) -> str:
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append(sql)
        sql = re.sub(r"\{(\w+):[^}]+\}", r":\1", sql).replace(" FINAL", "")
        sql = sql.replace(
            "toStartOfInterval(bucket_start, INTERVAL :bucket_seconds SECOND)",
            "toStartOfMinute(bucket_start)",
        )
        sql = sql.replace("LIMIT 10 BY dimension", "")
        bindings = {
            key: self.stamp(value) if isinstance(value, datetime) else value for key, value in (params or {}).items()
        }
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
        [
            start - tick,
            start,
            start + tick,
            end - tick,
            end,
            end + tick,
            end + timedelta(seconds=30),
        ]
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
        "SELECT count(*) FROM request_facts "
        "WHERE service_id = 'svc' AND publication_state = 'visible' "
        "AND batch_id = 'facts' "
        "AND event_timestamp >= ? AND event_timestamp < ?",
        (client.stamp(start), client.stamp(end)),
    ).fetchone()[0]

    response = entrypoint(
        service,
        AggregatesRequest(
            fields=["url", "country"],
            sections=["core", "topten"],
            chart_metric="requests",
            chart_interval="1 minute",
        ),
        start.isoformat(),
        end.isoformat(),
    )
    if entrypoint is bundle:
        response = response["aggregates"]

    assert response.total_rows == response.total_rows_total == expected
    for field in ("url", "country"):
        assert response.data[field].total == expected
        assert sum(row.count for row in response.data[field].top) == expected
    assert sum(row.count for row in response.map_data) == expected
    assert sum(row.value for row in response.time_series) == expected

    if any(value.second or value.microsecond for value in (start, end)):
        assert client.calls
        assert all("FROM request_facts" in sql for sql in client.calls)
    else:
        assert any("FROM request_aggregates" in sql for sql in client.calls)


@pytest.mark.parametrize("event_window", [_WINDOWS[4]], indirect=True)
@pytest.mark.parametrize("batched", [False, True])
def test_request_projection_excludes_the_end_bucket(event_window, batched) -> None:
    service, start, end = event_window
    watermark = service.watermark_for("request")
    if batched:
        responses = query_clickhouse_aggregates_batch(
            service.client,
            service_id="svc",
            domain="request",
            dimensions=["url", "country"],
            start=start,
            end=end,
            watermark=watermark,
        )
    else:
        responses = {
            "url": query_clickhouse_aggregate(
                service.client,
                AggregateRequest("svc", "request", start, end, "url"),
                watermark=watermark,
            )
        }

    for response in responses.values():
        assert response.request_count == 3
        assert sum(count for _, count in response.top_values) == 3


# ── Merged from tests/high_scale/test_performance.py ──

_PERF_START = "2026-09-16T14:00:00Z"
_PERF_END = "2026-09-16T15:00:00Z"

_PERCENTILE_ROW = {
    "value": "/slow",
    "req_count": 9,
    "avg_ms": 12.5,
    "p50_ms": 10.0,
    "p95_ms": 20.0,
    "p99_ms": 30.0,
}


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


def test_waterfall_is_the_summary_weighted_average_not_an_empty_stub():
    client = _PerfClient(
        {
            "waterfall": [
                {
                    "component_count": 4,
                    "edge_processing": 12.5,
                    "origin_wait": 40.0,
                    "origin_download": 5.0,
                    "client_download": 9.25,
                }
            ]
        }
    )

    res = _run_perf(client, sections={"waterfall", "scatter"})

    assert res.waterfall.avg is not None
    assert res.waterfall.avg.model_dump() == {
        "edge_processing": 12.5,
        "origin_wait": 40.0,
        "origin_download": 5.0,
        "client_download": 9.25,
    }
    sql, params = client.sql_for("waterfall")
    assert "FROM performance_minute_summary" in sql
    assert "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0)" in sql
    assert "publication_state='visible'" in sql
    assert params["service_id"] == "svc"


def test_unfiltered_waterfall_does_not_alias_an_aggregate_over_the_column_it_divides_by():
    client = _PerfClient({"waterfall": [{"n_components": 1, "edge_processing": 1.0}]})
    _run_perf(client, sections={"waterfall"})
    sql, _ = client.sql_for("waterfall")
    assert "AS component_count" not in sql


def test_waterfall_with_no_rows_reports_zero_averages_like_the_standard_path():
    res = _run_perf(
        _PerfClient(
            {
                "waterfall": [
                    {
                        "component_count": 0,
                        "edge_processing": None,
                        "origin_wait": None,
                        "origin_download": None,
                        "client_download": None,
                    }
                ]
            }
        ),
        sections={"waterfall"},
    )
    assert res.waterfall.avg is not None
    assert res.waterfall.avg.model_dump() == dict.fromkeys(
        ("edge_processing", "origin_wait", "origin_download", "client_download"), 0.0
    )


def test_scatter_is_the_uniform_300_row_sample_of_hit_and_miss_points():
    client = _PerfClient(
        {
            "scatter": [
                {"origin_ms": 100.0, "edge_ms": 150.0, "cache": "MISS"},
                {"origin_ms": 20.0, "edge_ms": 100.0, "cache": "HIT"},
                {"origin_ms": 5.0, "edge_ms": 1.0, "cache": ""},
            ]
        }
    )

    res = _run_perf(client, sections={"scatter", "waterfall"})

    assert [(p.origin, p.edge, p.cache) for p in res.scatter] == [
        (100.0, 150.0, "MISS"),
        (20.0, 100.0, "HIT"),
        (5.0, 1.0, None),
    ]
    sql, params = client.sql_for("scatter")
    assert "FROM performance_scatter_samples" in sql
    assert "ORDER BY sample_key" in sql
    assert params["scatter_limit"] == 300


def test_top_asns_carry_resolved_labels_and_ints(monkeypatch):
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names", lambda service_id, asns: {7922: "Comcast Cable"} if asns else {}
    )
    row = {**_PERCENTILE_ROW, "value": "7922", "req_count": 40}
    res = _run_perf(_PerfClient({"top_asns": [row, {**row, "value": "64512"}]}), sections={"top_asns", "top_urls"})

    assert [a.asn for a in res.top_asns] == [7922, 64512]
    assert res.top_asns[0].label == "Comcast Cable (7922)"
    assert res.top_asns[1].label == "AS64512"


@pytest.mark.parametrize("filtered", [False, True])
def test_asn_enrichment_is_disjoint_overhead_and_cache_reuse_is_historical(
    monkeypatch: pytest.MonkeyPatch, filtered: bool
) -> None:
    from backend.core.high_scale_sections import current_clickhouse_scope
    from backend.core.query_attribution import Attribution, current_attribution

    from backend.utils.telemetry import get_page_load_id, set_page_load_id

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
    query = timings["performance.top_asns"]
    enrichment = timings["performance.asn_labels"]
    assert query["source"] == ("fact" if filtered else "projection")
    assert query["activity"] == "compute"
    assert query["time_ms"] == pytest.approx(10)
    assert enrichment["source"] == "unknown"
    assert enrichment["activity"] == "overhead"
    assert enrichment["time_ms"] == pytest.approx(40)
    assert enrichment["executor_dispatch"] == "inline"
    assert enrichment["executor_wait_ms"] is None
    assert scopes["query"]["section_id"] == query["section_id"]
    assert scopes["labels"]["section_id"] == enrichment["section_id"]
    assert query["section_id"] != enrichment["section_id"]
    for row in timings.values():
        assert row["service_id"] == "svc"
        assert row["request_id"] == "request-perf"
        assert row["page_load_id"] == "page-perf"
        assert row["server_cpu_ms"] is None
    assert response.top_asns[0].label == "Comcast Cable (7922)"
    assert hit.response_cache.state == "hit"
    assert hit.section_timings == response.section_timings
    assert len(client.calls) == 1
    assert clock[0] == pytest.approx(0.05)
    assert current_clickhouse_scope()["section_id"] is None


def test_projection_latencies_are_converted_from_stored_microseconds_to_milliseconds():
    client = _PerfClient({"top_urls": [_PERCENTILE_ROW]})
    res = _run_perf(client, sections={"top_urls", "top_asns"})
    url = res.top_urls[0]
    assert (url.avg, url.p50, url.p95, url.p99) == (12.5, 10.0, 20.0, 30.0)
    sql, _ = client.sql_for("top_urls")
    assert sql.count("/ 1000.0") == 4


@pytest.mark.parametrize(
    ("sort_by", "column"), [("avg", "avg_ms"), ("p50", "p50_ms"), ("p95", "p95_ms"), ("p99", "p99_ms")]
)
def test_sort_by_orders_the_top_tables(sort_by, column):
    client = _PerfClient()
    _run_perf(client, _perf_req(sort_by=sort_by), sections={"top_urls", "top_asns"})
    for marker in ("top_urls", "top_asns"):
        sql, _ = client.sql_for(marker)
        assert f"ORDER BY {column} DESC NULLS LAST" in sql


def test_top_asns_require_more_than_ten_requests_like_the_standard_path():
    client = _PerfClient()
    _run_perf(client, sections={"top_urls", "top_asns"})
    assert "HAVING req_count > 5" in client.sql_for("top_urls")[0]
    assert "HAVING req_count > 10" in client.sql_for("top_asns")[0]


def test_sections_prune_queries_and_pairs_are_expected_from_the_router():
    client = _PerfClient()
    res = _run_perf(client, sections={"ttl_dist"})
    assert client.markers == {"ttl_dist"}
    assert res.top_urls == [] and res.top_asns == [] and res.scatter == []
    assert res.waterfall.avg is None


def test_none_sections_serve_every_section():
    client = _PerfClient()
    _run_perf(client, sections=None)
    assert client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}


def test_ttl_dist_uses_every_ttl_value_and_the_standard_bucket_edges():
    client = _PerfClient(
        {
            "ttl_dist": [
                {"bucket": "0s", "count": 3, "min_ttl": 0},
                {"bucket": "<30m", "count": 7, "min_ttl": 1201},
            ]
        }
    )
    res = _run_perf(client, sections={"ttl_dist"})
    assert [(b.bucket, b.count) for b in res.ttl_dist] == [("0s", 3), ("<30m", 7)]
    sql, _ = client.sql_for("ttl_dist")
    assert "FROM request_aggregates" in sql
    assert "LIMIT" not in sql
    for edge in ("'<30m'", "'<3h'", "'<1w'", "'<1y'", "'>1y'"):
        assert edge in sql


def test_unfiltered_requests_read_only_projection_tables():
    client = _PerfClient()
    _run_perf(client)
    assert not any("request_facts" in sql for sql, _ in client.calls)
    for sql, params in client.calls:
        assert "high_scale_batch_publications" in sql
        assert params["start"].isoformat().startswith("2026-09-16T14:00:00")


def test_filters_are_applied_against_request_facts_for_every_section():
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
            ],
            "scatter": [{"origin_ms": 1.0, "edge_ms": 2.0, "cache": "HIT"}],
        }
    )
    req = _perf_req(filters={"country": FilterSpec(mode="include", values=["US"])})
    res = _run_perf(client, req)
    assert res.top_urls[0].p99 == 30.0
    assert res.waterfall.avg is not None and res.waterfall.avg.client_download == 4.0
    assert res.scatter[0].cache == "HIT"
    assert res.approx is False
    assert client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}
    for sql, params in client.calls:
        assert "FROM request_facts" in sql
        assert "performance_minute" not in sql and "performance_scatter" not in sql
        assert "country IN ({filter_0_0:String})" in sql
        assert params["filter_0_0"] == "US"
        assert "publication_state='visible'" in sql
        assert "event_timestamp >=" in sql
    assert "ORDER BY cityHash64(event_id)" in client.sql_for("scatter")[0]


def test_filtered_sort_by_is_honored():
    client = _PerfClient()
    _run_perf(
        client,
        _perf_req(filters={"asn": FilterSpec(mode="exclude", values=["64512"])}, sort_by="avg"),
        sections={"top_urls", "top_asns"},
    )
    sql, _ = client.sql_for("top_urls")
    assert "ORDER BY avg_ms DESC NULLS LAST" in sql
    assert "NOT IN" in client.sql_for("top_asns")[0]


def test_filter_on_a_field_without_a_facts_mapping_is_rejected_not_ignored():
    with pytest.raises(ValueError, match="unsupported"):
        _run_perf(_PerfClient(), _perf_req(filters={"not_a_field": FilterSpec(mode="include", values=["x"])}))


def test_empty_filter_values_do_not_force_the_filtered_path():
    client = _PerfClient()
    _run_perf(client, _perf_req(filters={"country": FilterSpec(mode="include", values=[])}), sections={"waterfall"})
    assert "FROM performance_minute_summary" in client.sql_for("waterfall")[0]


def test_response_is_marked_approximate_and_validates_against_the_wire_model():
    res = _run_perf(_PerfClient({"top_urls": [_PERCENTILE_ROW]}))
    assert isinstance(res, PerformanceAggregatesResponse)
    assert res.approx is True


def _perf_event(**overrides):
    event = {
        "event_id": "evt-1",
        "timestamp": "2026-09-16T14:00:01Z",
        "url": "/a",
        "asn": "7922",
        "cache": "MISS",
        "elapsed": 250_000,
        "ttfb": 0.1,
        "ottfb": 40_000,
        "ottlb": 90_000,
    }
    event.update(overrides)
    return event


def test_summary_rows_decompose_latency_into_waterfall_components_in_ms():
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
        },
        {
            "bucket_start": "2026-09-16T14:01:00+00:00",
            "component_count": 1,
            "edge_processing_sum_ms": 60.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 150.0,
        },
    )


def test_components_are_clamped_at_zero_like_the_standard_path():
    projection = build_performance_projection_rows(
        (_perf_event(elapsed=10_000, ttfb=0.001, ottfb=40_000, ottlb=20_000),)
    )
    row = projection.summary_rows[0]
    assert row["edge_processing_sum_ms"] == 0.0
    assert row["origin_download_sum_ms"] == 0.0
    assert row["client_download_sum_ms"] == 0.0
    assert row["origin_wait_sum_ms"] == 40.0


def test_rows_without_ttfb_or_elapsed_never_enter_summary_or_scatter():
    projection = build_performance_projection_rows(
        (_perf_event(ttfb=None), _perf_event(event_id="e2", elapsed=""), _perf_event(event_id="e3", ttfb="nan"))
    )
    assert projection.summary_rows == ()
    assert projection.scatter_rows == ()


def test_scatter_rows_carry_origin_edge_and_cache_and_drop_negative_edge():
    projection = build_performance_projection_rows(
        (
            _perf_event(),
            _perf_event(event_id="evt-hit", cache="HIT", elapsed=120_000, ttfb=0.02),
            _perf_event(event_id="evt-neg", elapsed=1_000, ttfb=0.5),
            _perf_event(event_id="evt-nocache", cache=None),
        )
    )
    shapes = {(row["cache"], row["origin_ms"], row["edge_ms"]) for row in projection.scatter_rows}
    assert shapes == {("MISS", 100.0, 150.0), ("HIT", 20.0, 100.0), ("", 100.0, 150.0)}
    assert all(0 <= row["sample_key"] < 2**64 for row in projection.scatter_rows)
    assert all(row["bucket_start"] == "2026-09-16T14:00:00+00:00" for row in projection.scatter_rows)


def test_scatter_keeps_the_lowest_sample_keys_so_merged_batches_stay_uniform():
    events = tuple(_perf_event(event_id=f"evt-{i}") for i in range(SCATTER_SAMPLE_SIZE * 2))
    projection = build_performance_projection_rows(events)
    assert len(projection.scatter_rows) == SCATTER_SAMPLE_SIZE
    keys = [row["sample_key"] for row in projection.scatter_rows]
    assert keys == sorted(keys)
    again = build_performance_projection_rows(tuple(reversed(events)))
    assert [row["sample_key"] for row in again.scatter_rows] == keys


def test_dimension_rows_are_unchanged():
    projection = build_performance_projection_rows((_perf_event(), _perf_event(event_id="e2", url="/b")))
    assert {(r["dimension"], r["value"]) for r in projection.dimension_rows} == {
        ("url", "/a"),
        ("url", "/b"),
        ("asn", "7922"),
    }


def test_integer_asn_from_the_vcl_log_format_still_produces_an_asn_dimension():
    projection = build_performance_projection_rows((_perf_event(asn=7922), _perf_event(event_id="evt-2", asn=15169)))
    asn_values = {row["value"] for row in projection.dimension_rows if row["dimension"] == "asn"}
    assert asn_values == {"7922", "15169"}


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


def test_until_caps_the_window_at_the_worker_deploy_time(script, monkeypatch, capsys) -> None:
    code, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--days", "7"]
    )
    assert code == 0
    assert seen["now"] == datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    assert seen["window"] == timedelta(days=7)
    assert seen["service_id"] == "svc"
    assert '"rebuild_id": "rb-1"' in capsys.readouterr().out


def test_incomplete_rebuild_exits_nonzero(script, monkeypatch) -> None:
    assert (
        _run_backfill_script(
            script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"], complete=False
        )[0]
        == 1
    )


def test_until_is_required_and_must_carry_an_offset(script, monkeypatch) -> None:
    with pytest.raises(SystemExit) as missing:
        _run_backfill_script(script, monkeypatch, ["--service-id", "svc"])
    with pytest.raises(SystemExit) as naive:
        _run_backfill_script(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00"])
    assert missing.value.code == 2
    assert naive.value.code == 2


def test_rebuild_id_is_generated_and_printed_before_any_work_so_a_killed_run_can_resume(
    script, monkeypatch, capsys
) -> None:
    _, seen = _run_backfill_script(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"])
    assert seen["rebuild_id"]
    assert f"rebuild_id={seen['rebuild_id']}" in capsys.readouterr().err


def test_an_explicit_rebuild_id_is_passed_through_unchanged(script, monkeypatch) -> None:
    _, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--rebuild-id", "abc123"]
    )
    assert seen["rebuild_id"] == "abc123"


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
    def lookup(_service_id, asns):
        return {a: _ASN_NAMES.get(a, f"AS{a}") for a in asns}

    with patch("backend.core.duckdb.get_asn_names", side_effect=lookup):
        yield


def test_quality_scatter_returns_sampled_rtt_vs_ttfb_points():
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
    assert "LIMIT 2000" in sql
    assert "toFloat64OrNull(custom_fields['ttfb']) > 0" in sql
    assert "toFloat64OrNull(custom_fields['tcp_rtt']) > 0" in sql
    assert params["service_id"] == "svc"


def test_quality_honors_filters_on_every_query_and_enriches_asn_labels():
    def responder(sql, params):
        if "toInt32OrNull(custom_fields['asn']) AS label" in sql:
            return [{"label": 7922, "rtt_ms": 20.0, "reqs": 10}]
        return []

    service, client = _net_service(responder)
    req = NetworkQualityRequest(
        start_time=_NET_START_S,
        end_time=_NET_END_S,
        filters={"country": FilterSpec(mode="include", values=["US", "CA"])},
    )
    resp = network_quality(service, req, _NET_START_S, _NET_END_S)

    assert len(client.calls) == 5
    for sql, params in client.calls:
        assert "country IN ({filter_0_0:String}, {filter_0_1:String})" in sql
        assert params["filter_0_0"] == "US" and params["filter_0_1"] == "CA"
    assert resp.by_asn[0].value == "7922"
    assert resp.by_asn[0].label == "Comcast Cable Communications (7922)"


def test_quality_scatter_query_failure_is_not_masked_as_empty():
    def responder(sql, params):
        if "AS cache_state" in sql:
            raise RuntimeError("clickhouse down")
        return []

    service, _ = _net_service(responder)
    with pytest.raises(RuntimeError, match="clickhouse down"):
        network_quality(service, NetworkQualityRequest(), _NET_START_S, _NET_END_S)


def test_pop_health_aggregates_per_pop_like_the_standard_live_sql():
    def responder(sql, params):
        return [
            {
                "pop": "SJC",
                "requests": 200,
                "errors": 10,
                "p50_rtt_us": 18000.0,
                "p95_ttfb_ms": 320.5,
                "cache_hits": 150,
                "bandwidth_bytes": 4096,
            },
            {
                "pop": "LHR",
                "requests": 50,
                "errors": 0,
                "p50_rtt_us": None,
                "p95_ttfb_ms": None,
                "cache_hits": 0,
                "bandwidth_bytes": None,
            },
        ]

    service, client = _net_service(responder)
    resp = get_pop_health(service, _NET_START, _NET_END)

    sjc, lhr = resp.data
    assert (sjc.pop, sjc.requests, sjc.errors, sjc.error_rate) == ("SJC", 200, 10, 5.0)
    assert (sjc.p50_rtt_us, sjc.p95_ttfb_ms, sjc.cache_hit_rate, sjc.bandwidth_bytes) == (18000.0, 320.5, 75.0, 4096)
    assert (lhr.p50_rtt_us, lhr.p95_ttfb_ms, lhr.cache_hit_rate, lhr.bandwidth_bytes) == (None, None, 0.0, 0)
    ((sql, params),) = client.calls
    assert "countIf(toInt32OrZero(custom_fields['status']) >= 400 OR toInt32OrZero(custom_fields['status']) = 0)" in sql
    assert "'HIT', 'HIT-STALE'" in sql
    assert params["start"] == _NET_START and params["end"] == _NET_END


def test_pop_health_defaults_to_trailing_24h_and_treats_naive_bounds_as_utc():
    service, client = _net_service()
    get_pop_health(service, None, None)
    _, params = client.calls[0]
    assert (params["end"] - params["start"]).total_seconds() == 86400

    service, client = _net_service()
    get_pop_health(service, datetime(2026, 9, 15, 0, 0), datetime(2026, 9, 15, 1, 0))
    _, params = client.calls[0]
    assert params["start"].tzinfo is UTC and params["end"].tzinfo is UTC


def test_health_threads_filters_map_asn_and_reports_throughput():
    def responder(sql, params):
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "throughput_bps": 1_250_000.4,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                }
            ]
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
        assert "country NOT IN ({filter_0_0:String})" in sql
        assert "15169" not in params.values()
    ((map_sql, map_params),) = client.matching("AS lat")
    assert "toInt32OrNull(custom_fields['asn']) = {map_asn:Int32}" in map_sql
    assert map_params["map_asn"] == 7922
    for needle in ("AS region", "GROUP BY asn, c_speed", "quantile(0.95)"):
        assert "map_asn" not in client.matching(needle)[0][0]


def test_health_shielding_analysis_is_computed_from_rid_prid_pairs():
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
                },
                {
                    "edge_pop": "LHR",
                    "shield_pop": "IAD",
                    "requests": 10,
                    "p50_ms": 300.0,
                    "p95_ms": 500.0,
                    "p99_ms": None,
                },
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
    assert shield.rows[0].p95_ms == 90.0 and shield.rows[1].p99_ms is None
    assert shield.rows[0].distance_km is not None
    assert (shield.total_routes, shield.truncated) == (2, False)
    assert len(client.calls) == 1
    sql, params = client.calls[0]
    edge_cte, shield_cte = sql.split("INNER JOIN")
    assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in edge_cte
    assert params["filter_k0"] == "pop"
    assert "filter_0_0" not in shield_cte


def test_health_shielding_distinguishes_edge_only_from_unmatched():
    def edge_only(sql, params):
        return [{"n": 0}] if "SELECT count() AS n" in sql else []

    service, _ = _net_service(edge_only)
    resp = network_health(service, NetworkHealthRequest(sections=["shielding_analysis"]), _NET_START_S, _NET_END_S)
    assert resp.shielding_analysis.has_data is False and resp.shielding_analysis.edge_only is True

    def unmatched(sql, params):
        return [{"n": 1}] if "SELECT count() AS n" in sql else []

    service, _ = _net_service(unmatched)
    resp = network_health(service, NetworkHealthRequest(sections=["shielding_analysis"]), _NET_START_S, _NET_END_S)
    assert resp.shielding_analysis.has_data is False and resp.shielding_analysis.edge_only is None


def test_health_skips_shielding_when_only_core_sections_requested():
    service, client = _net_service()
    network_health(service, NetworkHealthRequest(sections=["map_buckets", "cities"]), _NET_START_S, _NET_END_S)
    assert client.calls
    assert not client.matching("INNER JOIN")


def test_transport_honors_filters_and_reports_missing_fields():
    def responder(sql, params):
        if "seen_tcp_rtt" in sql:
            return [
                {
                    "total_reqs": 100,
                    "total_bytes": 10,
                    "rtt_p50": 10.0,
                    "rtt_p90": 20.0,
                    "rtt_p95": 30.0,
                    "rtt_p99": 40.0,
                    "avg_retrans": None,
                    "avg_ploss_pct": 0.5,
                    "avg_del_rate_mbps": None,
                    "seen_tcp_rtt": 100,
                    "seen_retrans": 0,
                    "seen_ploss": 80,
                    "seen_delivery_rate": 0,
                }
            ]
        return []

    service, client = _net_service(responder)
    resp = get_transport_metrics(service, _NET_START_S, _NET_END_S, {"pop": FilterSpec(mode="include", values=["SJC"])})

    assert resp.missing_fields == ["retrans", "delivery_rate"]
    assert resp.has_transport_data is True
    assert len(client.calls) == 2
    for sql, params in client.calls:
        assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in sql
        assert params["filter_k0"] == "pop"
        assert params["filter_0_0"] == "SJC"


def test_transport_with_no_logged_fields_flags_no_transport_data():
    def responder(sql, params):
        if "seen_tcp_rtt" in sql:
            return [{"total_reqs": 5, "seen_tcp_rtt": 0, "seen_retrans": 0, "seen_ploss": 0, "seen_delivery_rate": 0}]
        return []

    service, _ = _net_service(responder)
    resp = get_transport_metrics(service, _NET_START_S, _NET_END_S)
    assert resp.has_transport_data is False
    assert resp.missing_fields == ["tcp_rtt", "retrans", "ploss", "delivery_rate"]


def test_bundle_returns_real_pop_health_and_scatter_and_passes_filters():
    def responder(sql, params):
        if "AS cache_state" in sql:
            return [{"rtt_ms": 10.0, "ttfb_ms": 50.0, "cache_state": "MISS"}]
        if "AS bandwidth_bytes" in sql:
            return [
                {
                    "pop": "SJC",
                    "requests": 10,
                    "errors": 1,
                    "p50_rtt_us": 1.0,
                    "p95_ttfb_ms": 2.0,
                    "cache_hits": 5,
                    "bandwidth_bytes": 7,
                }
            ]
        return []

    service, client = _net_service(responder)
    req = NetworkBundleRequest(
        start_time=_NET_START_S,
        end_time=_NET_END_S,
        filters={"country": FilterSpec(mode="include", values=["US"])},
    )
    resp = network_bundle(service, req, _NET_START_S, _NET_END_S)

    assert resp.pop_health and resp.pop_health[0].pop == "SJC"
    assert resp.quality.scatter[0].cache == "MISS"
    assert resp.transport is not None and resp.health is not None
    for sql, params in client.matching("AS cache_state") + client.matching("seen_tcp_rtt"):
        assert params["filter_0_0"] == "US"
    ((_, pop_params),) = client.matching("AS bandwidth_bytes")
    assert "filter_0_0" not in pop_params


def test_bundle_honors_section_selection():
    service, client = _net_service()
    req = NetworkBundleRequest(start_time=_NET_START_S, end_time=_NET_END_S, sections=["pop_health"])
    resp = network_bundle(service, req, _NET_START_S, _NET_END_S)
    assert resp.health is None and resp.quality is None and resp.transport is None
    assert resp.pop_health == []
    assert len(client.calls) == 1
    observation = resp.component_observations["pop_health"]
    assert observation.response_cache.state == "bypass"
    assert observation.section_timings
    assert observation.section_timings[0].model_extra["source"] == "fact"


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
    "call",
    [
        lambda s: network_health(
            s,
            NetworkHealthRequest(start_time=_NET_START_S, end_time=_NET_END_S, sections=["heatmap"]),
            _NET_START_S,
            _NET_END_S,
        ),
        lambda s: network_quality(
            s, NetworkQualityRequest(start_time=_NET_START_S, end_time=_NET_END_S), _NET_START_S, _NET_END_S
        ),
        lambda s: get_transport_metrics(s, _NET_START_S, _NET_END_S),
    ],
    ids=["health", "quality", "transport"],
)
def test_clickhouse_failures_propagate_instead_of_reading_as_empty_data(call) -> None:
    with pytest.raises(RuntimeError, match="clickhouse down"):
        call(_failing_service())


@pytest.mark.parametrize(
    ("marker", "call"),
    [
        (
            "GROUP BY label",
            lambda s: network_quality(
                s, NetworkQualityRequest(start_time=_NET_START_S, end_time=_NET_END_S), _NET_START_S, _NET_END_S
            ),
        ),
        ("proto_clean", lambda s: get_transport_metrics(s, _NET_START_S, _NET_END_S)),
    ],
    ids=["quality-dimension", "transport-protocol-share"],
)
def test_a_single_failing_query_is_not_hidden_behind_a_partial_response(marker, call) -> None:
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
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 7922,
                    "bucket_ts": _B1,
                    "reqs": 80,
                    "rtt_med_us": 21000.0,
                    "rtt_baseline_us": 18500.0,
                    "rtt_congestion_us": 2500.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1200.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": _B0,
                    "reqs": 60,
                    "rtt_med_us": 90000.0,
                    "rtt_baseline_us": 30000.0,
                    "rtt_congestion_us": 150000.0,
                    "avg_ploss": 0.03,
                    "rtt_jitter_us": 80000.0,
                    "error_pct": 8.0,
                },
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "lat": 37.33,
                    "lon": -121.89,
                    "metro": "807",
                    "bucket_ts": _B0,
                    "rtt_med_us": 20000.0,
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "reqs": 100,
                },
                {
                    "country": "US",
                    "city": "boston",
                    "lat": 42.36,
                    "lon": -71.06,
                    "metro": "506",
                    "bucket_ts": _B1,
                    "rtt_med_us": 90000.0,
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "reqs": 60,
                },
            ]
        if "AS region" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "region": "California",
                    "metro": "807",
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "total_reqs": 100,
                },
                {
                    "country": "US",
                    "city": "boston",
                    "region": "Massachusetts",
                    "metro": "506",
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "total_reqs": 60,
                },
            ]
        if "GROUP BY asn, c_speed" in sql:
            return [
                {"asn": 7922, "c_speed": "fast", "cnt": 150},
                {"asn": 7922, "c_speed": "medium", "cnt": 30},
                {"asn": 15169, "c_speed": "slow", "cnt": 60},
            ]
        if "quantile(0.95)" in sql:
            return [
                {"asn": 7922, "p95": 40000.0, "p99": 60000.0},
                {"asn": 15169, "p95": 180000.0, "p99": 260000.0},
            ]
        if "AS label" in sql:
            return [
                {"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160},
            ]
        return []


def _health_service() -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=None,
    )


def test_network_health_builds_full_clickhouse_payload():
    resp = network_health(_health_service(), _Req(), _Req.start_time, _Req.end_time)
    assert resp.buckets and len(resp.buckets) == 13
    assert resp.heatmap
    first = resp.heatmap[0]
    assert first.buckets and first.asn == 7922
    assert resp.leaderboard
    lead = {row.asn: row for row in resp.leaderboard}
    assert lead[7922].health_score_now is not None
    assert lead[15169].health_score_now < lead[7922].health_score_now
    assert lead[7922].p95_rtt_us == 40000.0
    assert lead[7922].c_speed_mix
    assert resp.map_buckets and resp.cities and any(c.name for c in resp.cities)
    assert resp.metro_leaderboard
    assert resp.summary is not None
    assert resp.summary.global_health_score != 100.0
    assert resp.summary.total_reqs == 240
    assert resp.summary.worst_asn is not None


def test_network_quality_populates_by_region():
    resp = network_quality(_health_service(), _Req(), _Req.start_time, _Req.end_time)
    assert resp.by_region and resp.by_country and resp.by_region[0].rtt_ms > 0
