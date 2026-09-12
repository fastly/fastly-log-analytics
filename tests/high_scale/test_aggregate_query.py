from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.archive_models import ServingWatermark

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.sql = ""
        self.params: dict[str, object] | None = None

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.sql = sql
        self.params = params
        return self.rows


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
    client = FakeClient([{"value": "/a", "aggregate_count": 3}, {"value": "/b", "aggregate_count": 2}])

    response = query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW),
        watermark=watermark(),
        now=NOW,
    )

    assert response.request_count == 5
    assert response.top_values == (("/a", 3), ("/b", 2))
    assert "request_aggregates" in client.sql
    assert "publication_state='visible'" in client.sql
    assert client.params is not None
    assert client.params["service_id"] == "svc"


@pytest.mark.parametrize(
    ("domain", "table", "metric"),
    [
        ("rum_vitals", "rum_vitals_aggregates", "event_count"),
        ("rum_errors", "rum_error_aggregates", "error_count"),
        ("cmcd", "cmcd_aggregates", "event_count"),
    ],
)
def test_domain_tables_are_isolated(domain: str, table: str, metric: str) -> None:
    client = FakeClient([])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", domain),
        watermark=watermark(domain),
        now=NOW,
    )

    assert table in client.sql
    assert f"sum({metric})" in client.sql


def test_watermark_must_match_request_domain() -> None:
    with pytest.raises(ValueError, match="watermark"):
        query_clickhouse_aggregate(
            FakeClient([]),
            AggregateRequest("svc", "request"),
            watermark=watermark("cmcd"),
        )
