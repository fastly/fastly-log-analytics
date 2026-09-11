import gzip
from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import (
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
)


class _ClickHouse:
    def __init__(self) -> None:
        self.batches = []

    def insert(self, batch):
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def test_source_flows_through_fenced_archive_and_visible_serving() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    owner = ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\nnot-json\n')

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert owner.owner_epoch == 1
    assert result.decoded.accepted_rows == 1
    assert result.decoded.quarantined_rows == 1
    assert result.publication.rows_visible == 1
    assert objects.exists(f"archive/manifests/{result.manifest.manifest_id}.commit")
    assert ledger.count() == 1
    assert clickhouse.batches[0].rows[0]["event_id"]

    replay = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )
    assert replay.publication.duplicate is True
    assert len(clickhouse.batches) == 1

    deletion = DeletionController(objects, ArchivePublication(objects), ledger)
    with pytest.raises(ValueError, match="grace period"):
        deletion.delete_source(
            result.manifest,
            current_owner_epoch=owner.owner_epoch,
            now=result.manifest.deletion_authorization_deadline - timedelta(seconds=1),
        )


def test_controller_refuses_sources_owned_by_another_data_plane() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    ownership.initialize("svc", owner="standard", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        worker_id="worker-1",
    )

    with pytest.raises(RuntimeError, match="not the owner"):
        controller.ingest(
            service_id="svc",
            domain="request",
            object_key="raw/request/one.gz",
            checksum="sha256:source",
            payload=b'{"url":"/ok"}\n',
        )
