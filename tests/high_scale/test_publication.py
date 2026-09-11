import pytest

from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    LostAcknowledgement,
    PartialInsert,
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
