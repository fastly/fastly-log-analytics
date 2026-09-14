from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.query_router import (
    QueryIntent,
    QueryRequest,
    QueryRouter,
    QueryRouterConfig,
    QueryTier,
    plan_query,
)

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def request(*, hours_old: int, hours: int = 1, intent: QueryIntent = QueryIntent.RAW) -> QueryRequest:
    end = NOW - timedelta(hours=hours_old)
    return QueryRequest("svc", "request", end - timedelta(hours=hours), end, intent)


def test_recent_triage_uses_aggregates() -> None:
    plan = plan_query(request(hours_old=2, intent=QueryIntent.TRIAGE), now=NOW)

    assert plan.tier is QueryTier.TRIAGE_AGGREGATE
    assert plan.sources == ("aggregates",)
    assert not plan.queued


def test_recent_raw_uses_clickhouse_facts() -> None:
    plan = plan_query(request(hours_old=2), now=NOW)

    assert plan.tier is QueryTier.RECENT_FACTS
    assert plan.sources == ("clickhouse_facts",)
    assert plan.bounded is False


def test_warm_history_reads_facts_and_archive() -> None:
    plan = plan_query(request(hours_old=24 * 10), now=NOW)

    assert plan.tier is QueryTier.WARM_FACTS_ARCHIVE
    assert plan.sources == ("clickhouse_facts", "fos_archive")


def test_cold_history_is_queued_and_bounded() -> None:
    plan = plan_query(request(hours_old=24 * 45, hours=2), now=NOW)

    assert plan.tier is QueryTier.COLD_JOB
    assert plan.queued
    assert plan.bounded
    assert plan.max_rows == 500


def test_cold_range_has_a_hard_bound() -> None:
    with pytest.raises(ValueError, match="cold query range"):
        plan_query(request(hours_old=24 * 45, hours=24 * 8), now=NOW)


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
        plan_query(request_value, now=NOW)


def test_future_ranges_are_rejected() -> None:
    future = QueryRequest("svc", "request", NOW - timedelta(minutes=1), NOW + timedelta(minutes=1))

    with pytest.raises(ValueError, match="future"):
        QueryRouter().plan(future, now=NOW)


def test_invalid_router_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="warm retention"):
        QueryRouter(QueryRouterConfig(warm_retention=timedelta(hours=1)))
