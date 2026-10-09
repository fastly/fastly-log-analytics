from __future__ import annotations

import gzip
from unittest.mock import MagicMock

import pytest

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import (
    HIGH_SCALE_PAGE_MAX_BYTES,
    BatchIngestResult,
    HighScaleIngestController,
)
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PublicationResult,
    PublicationStatus,
)
from backend.high_scale.source_discovery import (
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)


class _ClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def _make_source_payload(i: int) -> tuple[SourceObjectDescriptor, bytes]:
    body = f'{{"timestamp":"2026-10-09T12:00:00Z","url":"/page/{i}","status":200}}\n'.encode()
    compressed = gzip.compress(body)
    desc = SourceObjectDescriptor(
        object_key=f"raw/request/part_{i:04d}.gz",
        checksum=f"sha256:chk_{i:04d}",
        size_bytes=len(compressed),
        version=f"v{i}",
    )
    return desc, compressed


def test_coordinator_batch_page_processes_50_objects() -> None:
    """Verifies that a page of 50 objects in batch mode yields:
    - 1 claim_sources_batch call with 50 keys
    - 1 ingest_batch call with 50 payloads and next_cursor
    - 0 calls to single-object ingest
    - PageRun with processed=50, failed=0, duplicates=0, missing=0.
    """
    page_items = [_make_source_payload(i) for i in range(50)]
    descriptors = tuple(desc for desc, _ in page_items)
    payload_map = {desc.object_key: data for desc, data in page_items}

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(descriptors, next_cursor="cursor-50")

    reader = MagicMock()
    reader.read_source_object.side_effect = lambda svc, domain, key: payload_map[key]

    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1, source_cursor="cursor-0")
    control.source_cursor_for.return_value = "cursor-0"
    control.claim_sources_batch.return_value = tuple(
        MagicMock(object_key=d.object_key, checksum=d.checksum, claimed=True) for d in descriptors
    )

    controller = MagicMock()
    controller.batch_capable = True
    manifest = MagicMock()
    manifest.manifest_id = "manifest-batch-50"
    publication = PublicationResult(
        batch_id="batch-50",
        status=PublicationStatus.VISIBLE,
        rows_visible=50,
        duplicate=False,
    )
    controller.ingest_batch.return_value = BatchIngestResult(
        manifest=manifest,
        publication=publication,
        total_events=50,
        quarantined_events=0,
        sources_count=50,
    )

    ownership = OwnershipStore()
    ledger = HighScaleLedger()

    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=ownership,
        ledger=ledger,
        worker_id="worker-batch-test",
        control_plane=control,
        page_size=100,
    )

    run = coordinator.run_page(service_id="svc-test", domain="request")

    assert run.discovered == 50
    assert run.processed == 50
    assert run.failed == 0
    assert run.duplicates == 0
    assert run.missing == 0

    control.claim_sources_batch.assert_called_once()
    claimed_keys = control.claim_sources_batch.call_args[0][1]
    assert len(claimed_keys) == 50
    assert claimed_keys == tuple(d.object_key for d in descriptors)

    controller.ingest_batch.assert_called_once()
    _, kwargs = controller.ingest_batch.call_args
    assert kwargs["service_id"] == "svc-test"
    assert kwargs["domain"] == "request"
    assert len(kwargs["sources_payloads"]) == 50
    assert kwargs["next_cursor"] == "cursor-50"

    assert controller.ingest.call_count == 0


def test_coordinator_batch_page_partial_failure_holds_cursor() -> None:
    """Verifies that when individual objects fail during reading,
    the remaining valid objects are ingested but next_cursor is withheld (None),
    holding the cursor until the failure is resolved.
    """
    page_items = [_make_source_payload(i) for i in range(50)]
    descriptors = tuple(desc for desc, _ in page_items)
    payload_map = {desc.object_key: data for desc, data in page_items}

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(descriptors, next_cursor="cursor-50")

    missing_key = descriptors[5].object_key
    error_key = descriptors[10].object_key

    def fake_read(svc: str, domain: str, key: str) -> bytes:
        if key == missing_key:
            raise SourceObjectMissingError(f"Missing {key}")
        if key == error_key:
            raise RuntimeError(f"Network error reading {key}")
        return payload_map[key]

    reader = MagicMock()
    reader.read_source_object.side_effect = fake_read

    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1, source_cursor="cursor-0")
    control.source_cursor_for.return_value = "cursor-0"
    control.claim_sources_batch.return_value = tuple(
        MagicMock(object_key=d.object_key, checksum=d.checksum, claimed=True) for d in descriptors
    )

    controller = MagicMock()
    controller.batch_capable = True
    manifest = MagicMock()
    manifest.manifest_id = "manifest-batch-48"
    publication = PublicationResult(
        batch_id="batch-48",
        status=PublicationStatus.VISIBLE,
        rows_visible=48,
        duplicate=False,
    )
    controller.ingest_batch.return_value = BatchIngestResult(
        manifest=manifest,
        publication=publication,
        total_events=48,
        quarantined_events=0,
        sources_count=48,
    )

    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=OwnershipStore(),
        ledger=HighScaleLedger(),
        worker_id="worker-batch-test",
        control_plane=control,
        page_size=100,
    )

    run = coordinator.run_page(service_id="svc-test", domain="request")

    assert run.discovered == 50
    assert run.processed == 48
    assert run.missing == 1
    assert run.failed == 1

    # Ingest was invoked with the 48 successful payloads and next_cursor=None!
    controller.ingest_batch.assert_called_once()
    _, kwargs = controller.ingest_batch.call_args
    assert len(kwargs["sources_payloads"]) == 48
    assert kwargs["next_cursor"] is None

    # advance_source_cursor was NOT called
    assert control.advance_source_cursor.call_count == 0


def test_controller_ingest_batch_end_to_end_with_postgres_control() -> None:
    """Verifies HighScaleIngestController.ingest_batch end-to-end:
    - Decodes all 50 sources
    - Writes 1 batch archive parquet file
    - Commits 1 batch manifest referencing 50 source entries
    - Publishes to ClickHouse
    - Invokes acknowledge_sources_batch with all 50 keys and next_cursor.
    """
    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    archive_state = MagicMock(state=ArchiveState.ARTIFACT_UPLOADING)
    control.register_archive_manifest.return_value = archive_state
    control.archive_manifest.side_effect = KeyError("not found yet")

    object_store = InMemoryObjectStore()
    archive_pub = ArchivePublication(object_store)
    clickhouse = _ClickHouse()
    serving_pub = ClickHousePublication(InMemoryBatchManifestStore(), clickhouse)

    controller = HighScaleIngestController(
        ownership=OwnershipStore(),
        ledger=HighScaleLedger(),
        archive=archive_pub,
        serving=serving_pub,
        control_plane=control,
        worker_id="worker-e2e-test",
        deletion_grace_seconds=60,
    )

    sources_payloads = [_make_source_payload(i) for i in range(50)]

    result = controller.ingest_batch(
        service_id="svc-test",
        domain="request",
        sources_payloads=sources_payloads,
        next_cursor="cursor-50",
    )

    assert result.sources_count == 50
    assert result.total_events == 50
    assert result.quarantined_events == 0
    assert result.manifest is not None
    assert len(result.manifest.sources) == 50

    # Validate source slices in the manifest
    for i, src in enumerate(result.manifest.sources):
        assert src.row_start == i
        assert src.row_count == 1
        assert src.key == f"raw/request/part_{i:04d}.gz"

    # Validate archive published (artifact parquet, manifest JSON, and commit marker)
    parquet_keys = [k for k in object_store._objects if k.endswith(".parquet")]
    assert len(parquet_keys) == 1
    assert len(object_store._objects) == 3

    # Validate control plane calls
    control.register_archive_manifest.assert_called_once()
    assert control.record_source_counts.call_count == 50
    control.acknowledge_sources_batch.assert_called_once()
    ack_args, ack_kwargs = control.acknowledge_sources_batch.call_args
    assert ack_args[0] == "svc-test"
    assert len(ack_args[1]) == 50
    assert ack_kwargs["manifest_id"] == result.manifest.manifest_id
    assert ack_kwargs["next_cursor"] == "cursor-50"
    assert ack_kwargs["domain"] == "request"

    # Validate ClickHouse received batch
    assert len(clickhouse.batches) >= 1
    total_ch_rows = sum(len(b.rows) for b in clickhouse.batches if b.domain == "request")
    assert total_ch_rows == 50


def test_controller_ingest_batch_ledger_mode() -> None:
    """Verifies HighScaleIngestController.ingest_batch in standalone ledger mode:
    - 10 sources are ingested into ledger
    - Ledger records append, archive, and acknowledgement for each source.
    """
    ownership = OwnershipStore()
    ownership.initialize("svc-test", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()

    for i in range(10):
        ledger.discover(
            "svc-test",
            "request",
            f"raw/request/part_{i:04d}.gz",
            f"sha256:chk_{i:04d}",
            size_bytes=100,
            version=f"v{i}",
        )
        claim = ledger.claim("svc-test", f"raw/request/part_{i:04d}.gz", "worker-1")
        assert claim.claimed

    object_store = InMemoryObjectStore()
    archive_pub = ArchivePublication(object_store)
    clickhouse = _ClickHouse()
    serving_pub = ClickHousePublication(InMemoryBatchManifestStore(), clickhouse)

    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=archive_pub,
        serving=serving_pub,
        control_plane=None,
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )

    sources_payloads = [_make_source_payload(i) for i in range(10)]
    result = controller.ingest_batch(
        service_id="svc-test",
        domain="request",
        sources_payloads=sources_payloads,
        next_cursor="cursor-10",
    )

    assert result.sources_count == 10
    assert result.total_events == 10

    # Verify all 10 sources in ledger are now acknowledged
    for i in range(10):
        src = ledger.source("svc-test", f"raw/request/part_{i:04d}.gz")
        assert src is not None
        assert src.status == "acknowledged"
        # authorize_source_delete checks that the recorded archive_manifest_id matches
        auth = ledger.authorize_source_delete(
            "svc-test",
            f"raw/request/part_{i:04d}.gz",
            result.manifest.manifest_id,
            current_owner_epoch=1,
        )
        assert auth.archive_manifest_id == result.manifest.manifest_id


def test_controller_ingest_batch_exceeds_byte_limit_raises() -> None:
    """Verifies that ingest_batch rejects pages exceeding HIGH_SCALE_PAGE_MAX_BYTES."""
    controller = HighScaleIngestController(
        ownership=OwnershipStore(),
        ledger=HighScaleLedger(),
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        worker_id="worker-1",
    )

    # Fake a payload tuple that exceeds 64MB
    huge_payload = b"x" * (HIGH_SCALE_PAGE_MAX_BYTES + 1)
    desc = SourceObjectDescriptor("raw/huge.gz", "sha256:huge", len(huge_payload), "v1")

    with pytest.raises(ValueError, match="exceeds maximum"):
        controller.ingest_batch(
            service_id="svc-test",
            domain="request",
            sources_payloads=[(desc, huge_payload)],
        )


def test_controller_ingest_batch_dead_letter_ratio_exceeded_raises() -> None:
    """Verifies that ingest_batch raises ValueError when quarantined dead letters exceed threshold."""
    ownership = OwnershipStore()
    ownership.initialize("svc-test", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()

    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        worker_id="worker-1",
    )

    # 1 valid event, 10 malformed lines -> 10/11 = 90% dead letters (> 5% threshold)
    bad_data = b'{"timestamp":"2026-10-09T12:00:00Z","url":"/valid"}\n' + b"bad_line\n" * 10
    compressed = gzip.compress(bad_data)
    desc = SourceObjectDescriptor("raw/bad.gz", "sha256:bad", len(compressed), "v1")

    with pytest.raises(ValueError, match="dead letter ratio .* exceeds maximum"):
        controller.ingest_batch(
            service_id="svc-test",
            domain="request",
            sources_payloads=[(desc, compressed)],
        )


def test_controller_ingest_batch_empty_payloads() -> None:
    """Verifies ingest_batch with empty payloads gracefully returns empty result."""
    controller = HighScaleIngestController(
        ownership=OwnershipStore(),
        ledger=HighScaleLedger(),
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        worker_id="worker-1",
    )

    result = controller.ingest_batch(
        service_id="svc-test",
        domain="request",
        sources_payloads=[],
    )

    assert result.sources_count == 0
    assert result.total_events == 0
    assert result.quarantined_events == 0
