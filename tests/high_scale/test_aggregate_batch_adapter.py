from datetime import UTC, datetime
from uuid import UUID

import pytest

from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.publication import HighScaleBatch, PartialInsert, _batch_uuid


def _batch(domain: str = "request_aggregate") -> HighScaleBatch:
    return HighScaleBatch(
        batch_id="batch-1",
        service_id="svc",
        domain=domain,
        generation="epoch-1",
        rows=(
            {
                "dimension": "url",
                "value": "/a",
                "bucket_start": datetime(2026, 9, 14, tzinfo=UTC).isoformat(),
                "count": 3,
            },
            {
                "dimension": "country",
                "value": "US",
                "bucket_start": datetime(2026, 9, 14, tzinfo=UTC).isoformat(),
                "count": 3,
            },
        ),
    )


def _origin_batch(domain: str) -> HighScaleBatch:
    row = {
        "bucket_start": datetime(2026, 9, 14, tzinfo=UTC).isoformat(),
        "requests": 3,
        "origin_5xx": 1,
        "status_count": 3,
        "origin_bytes": 100,
        "latency_count": 3,
        "ttlb_count": 3,
        "overhead_count": 3,
        "origin_bytes_count": 3,
        "latency_p50_us": 1000.0,
        "latency_p95_us": 2000.0,
        "latency_p99_us": 2200.0,
    }
    if domain == "origin_summary":
        row.update(
            {
                "misses": 2,
                "passes": 1,
                "latency_p75_us": 1500.0,
                "ttlb_p50_us": 1800.0,
                "ttlb_p95_us": 2800.0,
                "cdn_overhead_p50_us": 500.0,
                "origin_bytes_p50": 100.0,
            }
        )
    else:
        row.update({"dimension": "url", "value": "/a"})
    return HighScaleBatch(
        batch_id=f"batch-{domain}",
        service_id="svc",
        domain=domain,
        generation="epoch-1",
        rows=(row,),
    )


class FakeHttpClickHouse:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[str], list[tuple]]] = []
        self.publications: dict[str, dict] = {}
        self.rows: dict[str, list[tuple]] = {}

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        if "high_scale_batch_publications" in sql:
            row = self.publications.get(str(params["batch_id"]))
            return [] if row is None else [row]
        if "SELECT count()" in sql:
            return [{"row_count": len(self.rows.get(str(params["batch_id"]), []))}]
        raise AssertionError(f"unexpected query: {sql}")

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        self.inserts.append((table, columns, rows))
        if table == "high_scale_batch_publications":
            row = rows[0]
            publication = dict(zip(columns, row, strict=True))
            self.publications[str(row[0])] = publication
        else:
            batch_id = str(rows[0][-2])
            self.rows.setdefault(batch_id, []).extend(rows)

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        self.rows.pop(str(batch_id), None)


def test_inserts_exact_allowlisted_shape() -> None:
    client = FakeHttpClickHouse()

    result = AggregateBatchAdapter(client).insert(_batch())

    assert result.rows_inserted == 2
    pending, facts, visible = client.inserts
    assert pending[0] == "high_scale_batch_publications"
    assert facts[0] == "request_aggregates"
    assert facts[1] == [
        "service_id",
        "bucket_start",
        "dimension",
        "value",
        "request_count",
        "batch_id",
        "publication_state",
    ]
    assert UUID(facts[2][0][-2])
    assert visible[2][0][7:9] == (1, "visible")


@pytest.mark.parametrize(
    ("domain", "table", "metric_column"),
    [
        ("rum_vitals_aggregate", "rum_vitals_aggregates", "event_count"),
        ("rum_errors_aggregate", "rum_error_aggregates", "error_count"),
        ("cmcd_aggregate", "cmcd_aggregates", "event_count"),
    ],
)
def test_maps_each_aggregate_domain_to_its_table(domain, table, metric_column) -> None:
    client = FakeHttpClickHouse()

    AggregateBatchAdapter(client).insert(_batch(domain))

    facts = client.inserts[1]
    assert facts[0] == table
    assert metric_column in facts[1]


def test_duplicate_retry_does_not_insert_twice() -> None:
    client = FakeHttpClickHouse()
    adapter = AggregateBatchAdapter(client)
    batch = _batch()

    first = adapter.insert(batch)
    second = adapter.insert(batch)

    assert first.rows_inserted == second.rows_inserted == 2
    fact_inserts = [call for call in client.inserts if call[0] == "request_aggregates"]
    assert len(fact_inserts) == 1


def test_partial_insert_is_cleaned_up_and_raised() -> None:
    client = FakeHttpClickHouse()
    batch = _batch()
    client.rows[_batch_uuid(batch)] = [("stale",)]

    with pytest.raises(PartialInsert):
        AggregateBatchAdapter(client).insert(batch)


def test_rejects_a_domain_outside_the_aggregate_allowlist() -> None:
    client = FakeHttpClickHouse()

    with pytest.raises(ValueError, match="domain"):
        AggregateBatchAdapter(client).insert(_batch("request"))


@pytest.mark.parametrize(
    ("domain", "table"),
    [
        ("origin_summary", "origin_minute_summary"),
        ("origin_dimensions", "origin_minute_dimensions"),
    ],
)
def test_maps_origin_projection_domains_to_allowlisted_tables(domain, table) -> None:
    client = FakeHttpClickHouse()

    result = AggregateBatchAdapter(client).insert(_origin_batch(domain))

    assert result.rows_inserted == 1
    facts = client.inserts[1]
    assert facts[0] == table
    assert facts[2][0][-2]
