"""Execute event-time predicates locally without ClickHouse or live services."""

import re
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.aggregate_query import (
    AggregateRequest,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.dashboard import aggregates, bundle
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import AggregatesRequest


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
