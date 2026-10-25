from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.high_scale._common import KeysetCursor
from backend.high_scale.archive_models import ServingWatermark
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

from backend.high_scale._filters import request_filter_clauses
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
from tests.high_scale import test_performance as performance_cases
from tests.high_scale import test_security as security_cases


def test_high_scale_performance_cache_observations_do_not_execute_the_repeat() -> None:
    client = performance_cases._Client()
    first = performance_cases._run(client, sections={"waterfall"})
    count = len(client.calls)
    assert count > 0
    assert first.response_cache.state == "miss"
    hit = performance_cases._run(client, sections={"waterfall"})
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
    from backend.high_scale import network
    from backend.models.network import NetworkQualityRequest

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
    from backend.high_scale import network
    from backend.models.network import NetworkBundleRequest

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

from backend.high_scale.historical_jobs import (
    HistoricalJob,
    HistoricalJobManager,
    HistoricalJobState,
)
from backend.high_scale.query_router import (
    QueryIntent,
    QueryPlan,
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
