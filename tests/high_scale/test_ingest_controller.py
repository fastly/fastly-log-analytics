import gzip
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.publication import (
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
)
from backend.high_scale.replay import replay_manifest
from backend.high_scale.source_discovery import SourceObjectDescriptor
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


class _ClickHouse:
    def __init__(self) -> None:
        self.batches = []

    def insert(self, batch):
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


class _PostgresControl:
    def __init__(self) -> None:
        self.owner_record = type("Owner", (), {"current_owner": "high_scale", "owner_epoch": 1})()
        self.calls: list[str] = []
        self.archive_state = type("State", (), {"state": ArchiveState.ARTIFACT_UPLOADING})()

    def owner(self, service_id: str):
        return self.owner_record

    def record_source_counts(self, service_id: str, object_key: str, **kwargs: object) -> None:
        self.calls.append("counts")

    def register_archive_manifest(self, manifest, *, owner_epoch: int):
        self.calls.append("register_manifest")
        return self.archive_state

    def archive_manifest(self, manifest_id: str):
        self.calls.append("archive_manifest_lookup")
        raise KeyError(manifest_id)

    def transition_archive_manifest(self, manifest_id: str, **kwargs: object):
        self.calls.append("transition_manifest")
        self.archive_state.state = kwargs["next_state"]
        return self.archive_state

    def acknowledge_sources_batch(self, service_id: str, object_keys, *, manifest_id: str, **kwargs: object) -> None:
        self.calls.append("acknowledged")


def _ingest_one(
    controller: HighScaleIngestController,
    *,
    service_id: str,
    domain: str,
    object_key: str,
    checksum: str,
    payload: bytes,
    version: str = "",
    now: datetime | None = None,
    max_dead_letter_ratio: float = 0.05,
):
    desc = SourceObjectDescriptor(object_key, checksum, len(payload), version)
    return controller.ingest_batch(
        service_id=service_id,
        domain=domain,
        sources_payloads=[(desc, payload)],
        now=now,
        max_dead_letter_ratio=max_dead_letter_ratio,
    )


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

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        max_dead_letter_ratio=1.0,
    )

    assert owner.owner_epoch == 1
    assert result.total_events == 1
    assert result.quarantined_events == 1
    assert result.publication.rows_visible == 1
    assert objects.exists(f"archive/manifests/{result.manifest.manifest_id}.commit")
    assert ledger.count() == 1
    assert clickhouse.batches[0].rows[0]["event_id"]
    archived_rows = pq.read_table(
        pa.BufferReader(ArchivePublication(objects).read_artifact(result.manifest))
    ).to_pylist()
    assert len(archived_rows) == 2
    assert any(row.get("_record_kind") == "dead_letter" for row in archived_rows)

    replay = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        max_dead_letter_ratio=1.0,
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


def test_ingest_writes_dimension_aggregates_alongside_facts() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    aggregate_clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregate_clickhouse),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(
        b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok","country":"US","ottfb":1200,"ost":503,"obytes":64}\n'
    )

    _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert len(aggregate_clickhouse.batches) == 5
    batch = aggregate_clickhouse.batches[0]
    assert batch.domain == "request_aggregate"
    values = {(row["dimension"], row["value"]) for row in batch.rows}
    assert ("url", "/ok") in values
    assert ("country", "US") in values
    summary = aggregate_clickhouse.batches[1]
    assert summary.domain == "origin_summary"
    assert summary.rows[0]["latency_p50_us"] == 1200
    dimensions = aggregate_clickhouse.batches[2]
    assert dimensions.domain == "origin_dimensions"
    assert ("url", "/ok") in {(row["dimension"], row["value"]) for row in dimensions.rows}


def test_ingest_tolerates_aggregate_publish_failure_without_failing_the_ingest() -> None:
    class _BoomAggregates:
        def publish(self, batch):
            raise RuntimeError("aggregate table unavailable")

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
        aggregates=_BoomAggregates(),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n')

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert result.publication.rows_visible == 1


def test_ingest_uses_the_ledgers_domain_not_the_callers_when_they_diverge() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    ledger.discover("svc", "rum_vitals", "raw/rum/one.gz", "sha256:source", size_bytes=16, version="v1")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="rum_errors",
        object_key="raw/rum/one.gz",
        checksum="sha256:source",
        payload=gzip.compress(b'{"metric_name":"LCP","metric_value":1.25}\n'),
        version="v1",
    )

    assert result.manifest.domain == "rum_vitals"
    assert clickhouse.batches[0].domain == "rum_vitals"


def test_ingest_filters_out_sibling_domain_rows_before_publishing() -> None:
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
    payload = gzip.compress(
        b'{"event_id":"vital-1","timestamp":"2026-09-14T20:00:00Z","metric_name":"LCP","metric_value":1.25}\n'
        b'{"event_id":"error-1","timestamp":"2026-09-14T20:00:01Z","error_message":"boom"}\n'
    )

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="rum_vitals",
        object_key="raw/rum/mixed.gz",
        checksum="sha256:mixed",
        payload=payload,
        version="v1",
    )

    assert result.total_events == 2
    assert len(clickhouse.batches) == 1
    assert len(clickhouse.batches[0].rows) == 1
    assert clickhouse.batches[0].rows[0]["metric_name"] == "LCP"
    assert result.publication.rows_visible == 1


def test_ingest_publishes_nothing_when_every_row_belongs_to_a_sibling_domain() -> None:
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
    payload = gzip.compress(b'{"event_id":"error-1","timestamp":"2026-09-14T20:00:01Z","error_message":"boom"}\n')

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="rum_vitals",
        object_key="raw/rum/all-errors.gz",
        checksum="sha256:all-errors",
        payload=payload,
        version="v1",
    )

    assert result.total_events == 1
    assert len(clickhouse.batches) == 0
    assert result.publication.rows_visible == 0


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

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/malformed.gz",
        checksum="sha256:malformed",
        payload=gzip.compress(b"not-json\n"),
        max_dead_letter_ratio=1.0,
    )

    assert result.total_events == 0
    assert result.quarantined_events == 1
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

    with pytest.raises(RuntimeError, match="owner is standard"):
        _ingest_one(
            controller,
            service_id="svc",
            domain="request",
            object_key="raw/request/one.gz",
            checksum="sha256:source",
            payload=b'{"url":"/ok"}\n',
        )


class _ManifestIdentityCheckingControl(_PostgresControl):
    def __init__(self) -> None:
        super().__init__()
        self._stored: dict[str, object] = {}

    def register_archive_manifest(self, manifest, *, owner_epoch: int):
        self.calls.append("register_manifest")
        stored = self._stored.setdefault(manifest.manifest_id, manifest)
        if stored != manifest:
            raise ValueError("archive manifest identity changed")
        return self.archive_state

    def archive_manifest(self, manifest_id: str):
        self.calls.append("archive_manifest_lookup")
        stored = self._stored.get(manifest_id)
        if stored is None:
            raise KeyError(manifest_id)
        return type("Record", (), {"manifest": stored, "owner_epoch": 1})()


def test_ingest_reuses_the_registered_manifest_on_retry_instead_of_rebuilding() -> None:
    control = _ManifestIdentityCheckingControl()
    clickhouse = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=control,
        ledger=HighScaleLedger(),
        control_plane=control,  # type: ignore[arg-type]
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n')

    first = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    second = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
    )

    assert second.manifest.manifest_id == first.manifest.manifest_id
    assert second.manifest.coverage_start == first.manifest.coverage_start
    assert second.manifest.coverage_end == first.manifest.coverage_end


def test_controller_can_use_postgres_control_plane_for_durable_state() -> None:
    control = _PostgresControl()
    clickhouse = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=control,
        ledger=HighScaleLedger(),
        control_plane=control,  # type: ignore[arg-type]
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    result = _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n'),
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert result.sources_count == 1
    assert control.calls == [
        "counts",
        "archive_manifest_lookup",
        "register_manifest",
        "transition_manifest",
        "transition_manifest",
        "transition_manifest",
        "acknowledged",
    ]


def test_ingest_publishes_performance_waterfall_and_scatter_projections() -> None:
    ownership = OwnershipStore()
    aggregate_clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregate_clickhouse),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(
        b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok","cache":"MISS","elapsed":250000,'
        b'"ttfb":0.1,"ottfb":40000,"ottlb":90000}\n'
    )

    _ingest_one(
        controller,
        service_id="svc",
        domain="request",
        object_key="raw/request/perf.gz",
        checksum="sha256:perf",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    by_domain = {batch.domain: batch for batch in aggregate_clickhouse.batches}
    summary = by_domain["performance_summary"].rows[0]
    assert summary["component_count"] == 1
    assert summary["origin_wait_sum_ms"] == 40.0
    scatter = by_domain["performance_scatter"].rows[0]
    assert (scatter["origin_ms"], scatter["edge_ms"], scatter["cache"]) == (100.0, 150.0, "MISS")
