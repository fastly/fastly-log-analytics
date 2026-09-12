import gzip
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
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
from backend.high_scale.replay import replay_manifest


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
    archived_rows = pq.read_table(
        pa.BufferReader(ArchivePublication(objects).read_artifact(result.manifest))
    ).to_pylist()
    assert len(archived_rows) == 2
    assert any(row.get("_record_kind") == "dead_letter" for row in archived_rows)

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

    rebuilt = _ClickHouse()
    replay_result = replay_manifest(
        result.manifest,
        ArchivePublication(objects),
        ClickHousePublication(InMemoryBatchManifestStore(), rebuilt),
    )
    assert replay_result.rows_read == 1
    assert len(rebuilt.batches) == 1
    assert rebuilt.batches[0].rows[0]["event_id"] == clickhouse.batches[0].rows[0]["event_id"]

    deletion = DeletionController(objects, ArchivePublication(objects), ledger)
    with pytest.raises(ValueError, match="grace period"):
        deletion.delete_source(
            result.manifest,
            current_owner_epoch=owner.owner_epoch,
            now=result.manifest.deletion_authorization_deadline - timedelta(seconds=1),
        )


def test_all_malformed_source_is_archived_without_serving_rows() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/malformed.gz",
        checksum="sha256:malformed",
        payload=gzip.compress(b"not-json\n"),
    )

    assert result.decoded.accepted_rows == 0
    assert result.decoded.quarantined_rows == 1
    assert result.publication.rows_visible == 0
    archived_rows = pq.read_table(
        pa.BufferReader(ArchivePublication(objects).read_artifact(result.manifest))
    ).to_pylist()
    assert len(archived_rows) == 1
    assert archived_rows[0]["_record_kind"] == "dead_letter"
    assert archived_rows[0]["line_ordinal"] == 0
    assert archived_rows[0]["raw_line_base64"] == "bm90LWpzb24="
    assert archived_rows[0]["source_object_key"] == "raw/request/malformed.gz"
    assert archived_rows[0]["reason"]


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
