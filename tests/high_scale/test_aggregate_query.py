from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.archive_models import ServingWatermark

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, responses: list[list[dict[str, object]]]) -> None:
        self.responses = list(responses)
        self.sql = ""
        self.params: dict[str, object] | None = None
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.calls.append((sql, params))
        self.sql = sql
        self.params = params
        return self.responses.pop(0)


def watermark(domain: str = "request") -> ServingWatermark:
    return ServingWatermark(
        "svc",
        domain,
        1,
        NOW - timedelta(hours=2),
        NOW - timedelta(minutes=1),
        "cursor",
        "archived",
        "visible",
        True,
    )


def test_query_reads_visible_rows_with_bound_values() -> None:
    client = FakeClient(
        [
            [{"total_count": 9}],
            [
                {"value": "/a", "aggregate_count": 3},
                {"value": "/b", "aggregate_count": 2},
            ],
        ],
    )

    response = query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW),
        watermark=watermark(),
        now=NOW,
    )

    assert response.request_count == 9
    assert response.top_values == (("/a", 3), ("/b", 2))
    assert "request_aggregates" in client.sql
    assert "publication_state='visible'" in client.sql
    assert "LIMIT 10" in client.sql
    assert client.params is not None
    assert client.params["service_id"] == "svc"
    assert len(client.calls) == 2
    assert "OVER ()" not in client.calls[1][0]


@pytest.mark.parametrize(
    ("domain", "table", "metric"),
    [
        ("rum_vitals", "rum_vitals_aggregates", "event_count"),
        ("rum_errors", "rum_error_aggregates", "error_count"),
        ("cmcd", "cmcd_aggregates", "event_count"),
    ],
)
def test_domain_tables_are_isolated(domain: str, table: str, metric: str) -> None:
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", domain),
        watermark=watermark(domain),
        now=NOW,
    )

    assert table in client.sql
    assert f"sum({metric})" in client.sql


def test_range_parameters_use_a_clickhouse_type_matching_the_parameter_format() -> None:
    """Regression test for a real bug found live: ClickHouseClient._parameter()
    always formats a datetime with microsecond precision (matching every
    other DateTime64(3) parameter in this codebase — see query_service.py),
    but this query annotated its range parameters as plain {..:DateTime}.
    ClickHouse's DateTime parser rejects a fractional-seconds suffix, so
    every range-bounded call failed with a live HTTP 500 (empty request
    windows never hit the range clause, so no fake-client unit test caught
    it)."""
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW),
        watermark=watermark(),
        now=NOW,
    )

    assert "{start:DateTime64(3)}" in client.sql
    assert "{end:DateTime64(3)}" in client.sql
    assert "{start:DateTime}" not in client.sql
    assert "{end:DateTime}" not in client.sql


def test_query_allows_digit_suffix_in_internal_dimension_identifier() -> None:
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", dimension="is_ipv6"),
        watermark=watermark(),
        now=NOW,
    )

    assert client.params is not None
    assert client.params["dimension"] == "is_ipv6"


@pytest.mark.parametrize("dimension", ["3xx", "ja-3", "url; DROP TABLE request_aggregates"])
def test_query_rejects_non_identifier_dimension(dimension: str) -> None:
    with pytest.raises(ValueError, match="internal identifier"):
        query_clickhouse_aggregate(
            FakeClient([[], []]),
            AggregateRequest("svc", "request", dimension=dimension),
            watermark=watermark(),
            now=NOW,
        )


def test_watermark_must_match_request_domain() -> None:
    with pytest.raises(ValueError, match="watermark"):
        query_clickhouse_aggregate(
            FakeClient([[], []]),
            AggregateRequest("svc", "request"),
            watermark=watermark("cmcd"),
        )
