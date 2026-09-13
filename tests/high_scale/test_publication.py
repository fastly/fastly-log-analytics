from uuid import UUID

import pytest

from backend.high_scale.publication import (
    BatchManifest,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    LostAcknowledgement,
    PartialInsert,
    PostgresBatchManifestStore,
    PublicationStatus,
)


def _batch() -> HighScaleBatch:
    return HighScaleBatch(
        batch_id="batch-1",
        service_id="svc",
        domain="request",
        generation="epoch-4",
        rows=({"event_id": "event-1"}, {"event_id": "event-2"}),
    )


class FakeClickHouse:
    def __init__(self) -> None:
        self.inserts: list[str] = []
        self.fail_after_insert = False
        self.partial_rows: int | None = None

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.inserts.append(batch.batch_id)
        if self.partial_rows is not None:
            raise PartialInsert(self.partial_rows)
        if self.fail_after_insert:
            self.fail_after_insert = False
            raise LostAcknowledgement
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def test_retry_after_lost_acknowledgement_publishes_once() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    client.fail_after_insert = True
    publication = ClickHousePublication(store, client)
    batch = _batch()

    with pytest.raises(LostAcknowledgement):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert result.rows_visible == 2
    assert client.inserts == ["batch-1", "batch-1"]
    assert store.get("batch-1").status is PublicationStatus.VISIBLE


def test_partial_insert_never_becomes_visible() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    client.partial_rows = 1
    publication = ClickHousePublication(store, client)

    with pytest.raises(PartialInsert):
        publication.publish(_batch())

    assert publication.is_visible("batch-1") is False
    assert store.get("batch-1").status is PublicationStatus.PENDING


class _DurableManifestControl:
    def __init__(self) -> None:
        self.manifests = {}

    def get_batch_manifest(self, batch_id: str):
        return self.manifests.get(batch_id)

    def put_batch_manifest(self, manifest) -> None:
        self.manifests[manifest.batch_id] = manifest


def test_postgres_manifest_store_survives_store_recreation() -> None:
    control = _DurableManifestControl()
    first_store = PostgresBatchManifestStore(control)
    first_store.put(
        BatchManifest(
            batch_id="batch-1",
            service_id="svc",
            domain="request",
            generation="epoch-4",
            digest="sha256:batch",
            expected_rows=2,
            status=PublicationStatus.VISIBLE,
            visible_rows=2,
        )
    )

    recreated_store = PostgresBatchManifestStore(control)

    manifest = recreated_store.get("batch-1")
    assert manifest is not None
    assert manifest.status is PublicationStatus.VISIBLE
    assert manifest.visible_rows == 2


def test_same_batch_id_with_different_digest_is_rejected() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    publication = ClickHousePublication(store, client)
    publication.publish(_batch())

    changed = HighScaleBatch(
        batch_id="batch-1",
        service_id="svc",
        domain="request",
        generation="epoch-4",
        rows=({"event_id": "different"},),
    )
    with pytest.raises(ValueError, match="digest"):
        publication.publish(changed)


def _request_batch() -> HighScaleBatch:
    return HighScaleBatch(
        batch_id="svc:request:object-1",
        service_id="svc",
        domain="request",
        generation="4",
        rows=(
            {
                "event_id": "event-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/request/object-1.gz",
                "source_object_version": "sha256:object-1",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "country": "US",
                "client_ip": "192.0.2.1",
                "url": "/",
                "custom_fields": {"tenant": "demo"},
                "cmcd": {},
            },
        ),
    )


class FakeHttpClickHouse:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[str], list[tuple]]] = []
        self.publications: dict[str, dict] = {}
        self.facts: dict[str, list[tuple]] = {}
        self.fail_fact_ack = False
        self.fail_visible_ack = False

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        if "high_scale_batch_publications" in sql:
            row = self.publications.get(str(params["batch_id"]))
            return [] if row is None else [row]
        if "SELECT count()" in sql:
            return [{"row_count": len(self.facts.get(str(params["batch_id"]), []))}]
        raise AssertionError(f"unexpected query: {sql}")

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        self.inserts.append((table, columns, rows))
        if table == "high_scale_batch_publications":
            row = rows[0]
            publication = dict(zip(columns, row, strict=True))
            self.publications[str(row[0])] = publication
            if publication["publication_state"] == "visible" and self.fail_visible_ack:
                self.fail_visible_ack = False
                raise RuntimeError("lost acknowledgement")
        else:
            batch_id = str(rows[0][4] if table == "cmcd_projection_facts" else rows[0][8])
            self.facts.setdefault(batch_id, []).extend(rows)
            if self.fail_fact_ack:
                self.fail_fact_ack = False
                raise RuntimeError("lost acknowledgement")


def test_clickhouse_adapter_inserts_exact_allowlisted_shape() -> None:
    client = FakeHttpClickHouse()
    result = ClickHouseBatchAdapter(client).insert(_request_batch())

    assert result.rows_inserted == 1
    pending, facts, visible = client.inserts
    assert pending[0] == "high_scale_batch_publications"
    assert pending[1] == [
        "batch_id",
        "service_id",
        "domain",
        "generation",
        "batch_digest",
        "expected_rows",
        "visible_rows",
        "quorum_acked",
        "publication_state",
        "manifest_version",
        "updated_at",
    ]
    assert facts[0] == "request_facts"
    assert facts[1][-5:] == ["country", "client_ip", "url", "custom_fields", "cmcd"]
    assert UUID(facts[2][0][1])
    assert UUID(facts[2][0][8])
    assert visible[2][0][7:9] == (1, "visible")


def test_clickhouse_adapter_rejects_invalid_rows_before_http() -> None:
    batch = _request_batch()
    invalid = HighScaleBatch(
        batch.batch_id,
        batch.service_id,
        batch.domain,
        batch.generation,
        ({**batch.rows[0], "custom_fields": {"bad": 1}},),
    )
    client = FakeHttpClickHouse()

    with pytest.raises(ValueError, match="keys and values"):
        ClickHouseBatchAdapter(client).insert(invalid)

    assert client.inserts == []


def test_clickhouse_adapter_duplicate_retry_does_not_insert_facts_twice() -> None:
    client = FakeHttpClickHouse()
    adapter = ClickHouseBatchAdapter(client)
    batch = _request_batch()

    first = adapter.insert(batch)
    second = adapter.insert(batch)

    assert first == second
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


def test_clickhouse_adapter_lost_visible_ack_is_recovered_by_retry() -> None:
    client = FakeHttpClickHouse()
    client.fail_visible_ack = True
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ClickHouseBatchAdapter(client))
    batch = _request_batch()

    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


def test_clickhouse_adapter_lost_fact_ack_is_recovered_without_duplicate_rows() -> None:
    client = FakeHttpClickHouse()
    client.fail_fact_ack = True
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ClickHouseBatchAdapter(client))
    batch = _request_batch()

    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


@pytest.mark.parametrize(
    ("domain", "row", "table"),
    [
        (
            "rum_vitals",
            {
                "event_id": "rum-vital-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/rum/vitals-1.gz",
                "source_object_version": "sha256:rum-1",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "metric_name": "LCP",
                "metric_value": 1.25,
            },
            "rum_vitals_facts",
        ),
        (
            "rum_errors",
            {
                "event_id": "rum-error-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/rum/errors-1.gz",
                "source_object_version": "sha256:rum-2",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "error_message": "boom",
            },
            "rum_error_facts",
        ),
        (
            "cmcd",
            {
                "request_event_id": "request-event-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "fields": {"br": "1000"},
            },
            "cmcd_projection_facts",
        ),
    ],
)
def test_clickhouse_adapter_maps_each_schema_domain(domain: str, row: dict, table: str) -> None:
    client = FakeHttpClickHouse()
    batch = HighScaleBatch("batch-" + domain, "svc", domain, "4", (row,))

    ClickHouseBatchAdapter(client).insert(batch)

    assert [name for name, _, _ in client.inserts].count(table) == 1
