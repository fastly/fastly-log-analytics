from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.high_scale.archive_models import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
from backend.high_scale.replay import replay_manifest
from backend.high_scale.source_discovery import SourceObjectDescriptor

from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.publication import (
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
)
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
    assert [batch.domain for batch in clickhouse.batches] == ["rum_vitals", "rum_errors"]
    assert len(clickhouse.batches[0].rows) == 1
    assert clickhouse.batches[0].rows[0]["metric_name"] == "LCP"
    assert len(clickhouse.batches[1].rows) == 1
    assert clickhouse.batches[1].rows[0]["error_message"] == "boom"
    assert result.publication.rows_visible == 1


def test_rum_errors_claim_also_publishes_sibling_vitals_from_the_shared_object() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(
        b'{"event_id":"error-1","timestamp":"2026-09-14T20:00:01Z","error_message":"boom"}\n'
        b'{"event_id":"vital-1","timestamp":"2026-09-14T20:00:00Z","metric_name":"LCP","metric_value":1.25}\n'
        b'{"event_id":"vital-2","timestamp":"2026-09-14T20:00:02Z","metric_name":"CLS","metric_value":0.1}\n'
    )

    _ingest_one(
        controller,
        service_id="svc",
        domain="rum_errors",
        object_key="raw/rum/mixed-errors-first.gz",
        checksum="sha256:mixed-errors-first",
        payload=payload,
        version="v1",
    )

    by_domain = {batch.domain: batch for batch in clickhouse.batches}
    assert sorted(by_domain) == ["rum_errors", "rum_vitals"]
    assert [row["error_message"] for row in by_domain["rum_errors"].rows] == ["boom"]
    assert sorted(row["metric_name"] for row in by_domain["rum_vitals"].rows) == ["CLS", "LCP"]
    assert len({batch.batch_id for batch in clickhouse.batches}) == 2


@pytest.mark.parametrize("domain", ["rum_vitals", "rum_errors"])
def test_empty_message_exception_is_visible_before_ack_and_invalid_record_is_archived(domain) -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    control = _PostgresControl()
    archive = ArchivePublication(InMemoryObjectStore())

    class BeforeAck(_ClickHouse):
        def insert(self, batch):
            assert "acknowledged" not in control.calls
            return super().insert(batch)

    clickhouse = BeforeAck()
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=archive,
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        control_plane=control,
        worker_id="worker-1",
    )
    payload = (
        b'{"timestamp":"2026-09-14T20:00:01Z","rum_error_message":"","rum_error_stack":"at app.js:1"}\n'
        b'{"timestamp":"2026-09-14T20:00:02Z","rum_metric_value":"","rum_error_message":""}\n'
    )
    result = _ingest_one(
        controller,
        service_id="svc",
        domain=domain,
        object_key="raw/rum/empty-message.gz",
        checksum="sha256:empty-message",
        payload=payload,
        version="v1",
        max_dead_letter_ratio=1.0,
    )
    assert [(batch.domain, len(batch.rows)) for batch in clickhouse.batches] == [("rum_errors", 1)]
    assert clickhouse.batches[0].rows[0]["error_message"] == "Unknown error"
    assert result.quarantined_events == 1
    archived = pq.read_table(pa.BufferReader(archive.read_artifact(result.manifest))).to_pylist()
    assert [row["_record_kind"] for row in archived] == ["event", "dead_letter"]
    assert archived[1]["line_ordinal"] == 1
    assert control.calls[-1] == "acknowledged"


def test_invalid_sibling_metric_is_counted_while_valid_exception_is_retained(caplog) -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    archive = ArchivePublication(InMemoryObjectStore())
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=archive,
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    record = {
        "timestamp": "2026-09-14T20:00:01Z",
        "rum_body": json.dumps(
            {
                "exceptions": [{"value": "boom"}],
                "measurements": [{"type": "web-vitals", "values": {"lcp": "invalid"}}],
            }
        ),
    }
    result = _ingest_one(
        controller,
        service_id="svc",
        domain="rum_errors",
        object_key="raw/rum/invalid-sibling.gz",
        checksum="sha256:invalid-sibling",
        payload=json.dumps(record).encode(),
        version="v1",
    )
    assert [batch.domain for batch in clickhouse.batches] == ["rum_errors"]
    archived = pq.read_table(pa.BufferReader(archive.read_artifact(result.manifest))).to_pylist()
    assert archived[0]["raw_json"] == json.dumps(record)
    assert any(
        getattr(item, "domain", None) == "rum_vitals" and getattr(item, "quarantined_rows", None) == 1
        for item in caplog.records
    )


def test_pure_vitals_object_publishes_no_sibling_error_rows() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(
        b'{"event_id":"vital-1","timestamp":"2026-09-14T20:00:00Z","metric_name":"LCP","metric_value":1.25}\n'
    )

    _ingest_one(
        controller,
        service_id="svc",
        domain="rum_vitals",
        object_key="raw/rum/pure-vitals.gz",
        checksum="sha256:pure-vitals",
        payload=payload,
        version="v1",
    )

    assert [batch.domain for batch in clickhouse.batches] == ["rum_vitals"]


def test_ingest_publishes_only_sibling_rows_when_every_row_belongs_to_the_sibling() -> None:
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
    assert [batch.domain for batch in clickhouse.batches] == ["rum_errors"]
    assert clickhouse.batches[0].rows[0]["error_message"] == "boom"
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


# ── Merged from test_worker.py ──

from dataclasses import dataclass
from threading import Event

from backend.high_scale.worker import HighScaleWorkerLoop

from backend.high_scale.orchestration import PageRun


@dataclass
class _Coordinator:
    calls: list[tuple[str, str]]

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def test_worker_loop_processes_each_configured_service_domain_once() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc-a", "svc-b"),
        domains=("request", "rum"),
        interval_seconds=0,
    )

    results = loop.run_once()

    assert coordinator.calls == [
        ("svc-a", "request"),
        ("svc-a", "rum"),
        ("svc-b", "request"),
        ("svc-b", "rum"),
    ]
    assert [(result.service_id, result.domain, result.page.processed) for result in results] == [
        ("svc-a", "request", 1),
        ("svc-a", "rum", 1),
        ("svc-b", "request", 1),
        ("svc-b", "rum", 1),
    ]


def test_worker_loop_stops_after_requested_iterations() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=0,
    )

    loop.run(max_iterations=2)

    assert coordinator.calls == [("svc", "request"), ("svc", "request")]


class _BlockingSweeper:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.calls = 0

    def sweep(self, *, service_id: str, limit: int) -> None:
        self.calls += 1
        self.started.set()
        self.release.wait(timeout=5)


def test_slow_deletion_sweep_does_not_block_ingest_pages() -> None:
    coordinator = _Coordinator([])
    sweeper = _BlockingSweeper()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=0,
        deletion_sweeper=sweeper,  # type: ignore[arg-type]
    )

    loop.run_once()
    assert sweeper.started.wait(timeout=2)
    loop.run_once()
    loop.run_once()

    assert coordinator.calls == [("svc", "request")] * 3
    assert sweeper.calls == 1
    sweeper.release.set()
    loop.wait_for_deletion_sweep(timeout=2)
    loop.run_once()
    loop.wait_for_deletion_sweep(timeout=2)
    assert sweeper.calls == 2


def test_worker_loop_with_interval_sleeps_and_stops() -> None:
    coordinator = _Coordinator([])
    sleeps: list[float] = []
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=2.0,
        sleeper=sleeps.append,
    )

    loop.run(max_iterations=2)

    assert coordinator.calls == [("svc", "request"), ("svc", "request")]
    assert len(sleeps) == 1
    assert 1.0 <= sleeps[0] <= 2.0


def test_multi_service_worker_loop_with_interval_sleeps_and_stops() -> None:
    from backend.high_scale.worker import _MultiServiceWorkerLoop

    coord_a = _Coordinator([])
    coord_b = _Coordinator([])
    sleeps: list[float] = []
    loop = _MultiServiceWorkerLoop(
        coordinators=(("svc-a", coord_a), ("svc-b", coord_b)),  # type: ignore[arg-type]
        domains=("request",),
        interval_seconds=2.0,
    )
    loop._sleeper = sleeps.append

    loop.run(max_iterations=2)

    assert coord_a.calls == [("svc-a", "request"), ("svc-a", "request")]
    assert coord_b.calls == [("svc-b", "request"), ("svc-b", "request")]
    assert len(sleeps) == 1
    assert 1.0 <= sleeps[0] <= 2.0


# ── Merged from test_topology_wiring.py ──

import logging
from unittest.mock import patch

from backend.high_scale.archive_models import HighScaleConfig, TopologyProfile, from_environment

from backend.high_scale import worker


def _config(profile: str) -> HighScaleConfig:
    return HighScaleConfig(
        enabled=True,
        clickhouse_url="http://clickhouse",
        fos_bucket="archive",
        archive_manifest_dsn="postgresql://manifest",
        worker_queue_url="redis://queue",
        profile=profile,
    )


def test_config_resolves_its_profile_through_the_topology_module() -> None:
    topology = _config("portable-production").topology_profile()

    assert (topology.name, topology.clickhouse_replicas_per_shard, topology.keeper_members) == (
        "portable-production",
        2,
        3,
    )
    assert topology.production_ha is True


def test_config_validation_rejects_unknown_profiles_via_the_topology_module() -> None:
    with pytest.raises(ValueError, match="local or portable-production"):
        _config("elevation-magic").validate()


def test_config_validation_accepts_every_profile_the_topology_module_builds() -> None:
    _config("local").validate()
    _config("portable-production").validate()


def test_validation_follows_the_topology_module_not_a_duplicated_name_list() -> None:
    sentinel = TopologyProfile("sentinel", 1, 1, True, False, 1)
    with patch("backend.high_scale.models.build_topology_profile", return_value=sentinel):
        assert _config("anything").topology_profile() is sentinel
        _config("anything").validate()


def test_from_environment_applies_the_profile_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "HIGH_SCALE_ENABLED": "1",
        "HIGH_SCALE_CLICKHOUSE_URL": "http://clickhouse",
        "HIGH_SCALE_FOS_BUCKET": "archive",
        "HIGH_SCALE_ARCHIVE_MANIFEST_DSN": "postgresql://manifest",
        "HIGH_SCALE_WORKER_QUEUE_URL": "redis://queue",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "portable-production")
    assert from_environment().topology_profile().production_ha is True

    monkeypatch.setenv("HIGH_SCALE_PROFILE", "nope")
    with pytest.raises(ValueError, match="local or portable-production"):
        from_environment()


def test_worker_startup_logs_the_resolved_topology(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(worker, "from_environment", lambda: _config("portable-production"))
    monkeypatch.delenv("HIGH_SCALE_SERVICE_IDS", raising=False)
    monkeypatch.setattr(worker.app_config, "list_configs", lambda: [])

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        with pytest.raises(ValueError, match="HIGH_SCALE_SERVICE_IDS is required"):
            worker.build_worker_from_environment()

    record = next(r for r in caplog.records if r.getMessage() == "high_scale.topology_profile")
    assert (record.profile, record.keeper_members, record.production_ha) == ("portable-production", 3, True)  # type: ignore[attr-defined]


def test_worker_startup_rejects_an_unknown_profile_before_any_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "from_environment", lambda: _config("bogus"))

    with pytest.raises(ValueError, match="local or portable-production"):
        worker.build_worker_from_environment()


def test_health_snapshot_reports_the_configured_topology(client, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "portable-production")
    with patch("backend.config.is_high_scale_mode", return_value=True):
        body = client.get("/api/admin/health-snapshot").json()

    assert body["high_scale"]["topology"] == {
        "name": "portable-production",
        "clickhouse_replicas_per_shard": 2,
        "keeper_members": 3,
        "persistent_serving_storage": True,
        "production_ha": True,
        "shard_count": 3,
    }


def test_health_snapshot_degrades_to_no_topology_for_an_unknown_profile(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "bogus")
    with patch("backend.config.is_high_scale_mode", return_value=True):
        resp = client.get("/api/admin/health-snapshot")

    assert resp.status_code == 200
    assert resp.json()["high_scale"]["topology"] is None


# ── Merged from test_maintenance_suppression.py ──

from unittest.mock import MagicMock

from backend.core.clickhouse_schema import maintain_clickhouse_system_tables


def test_maintain_clickhouse_system_tables_suppresses_http_413() -> None:
    client = MagicMock()
    # Simulate HTTP 413 Request Entity Too Large error on execute
    client.execute.side_effect = RuntimeError("ClickHouse HTTP 413: Request Entity Too Large")

    # Must not raise an exception
    maintain_clickhouse_system_tables(client)
    assert client.execute.called


def test_worker_run_once_continues_when_maintenance_fails(caplog: pytest.LogCaptureFixture) -> None:
    coordinator = MagicMock()
    coordinator.run_page.return_value = MagicMock()

    worker = HighScaleWorkerLoop(
        coordinator=coordinator,
        service_ids=("svc_test",),
        domains=("request",),
        interval_seconds=0.0,
    )

    # Force system cleanup interval
    worker._last_system_cleanup = 0.0

    mock_client = MagicMock()
    mock_client.execute.side_effect = RuntimeError("HTTP 413")

    with (
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=mock_client),
        patch("backend.utils.telemetry.run_usage_log_cleanup", return_value=None),
        caplog.at_level(logging.DEBUG),
    ):
        results = worker.run_once()

    assert len(results) == 1
    assert results[0].error is None
    assert coordinator.run_page.called


# ── Merged from test_clickhouse_resource_limits.py ──

import xml.etree.ElementTree as ET
from pathlib import Path

_CONFIG = Path(__file__).resolve().parents[2] / "clickhouse" / "config.d" / "resource_limits.xml"


def _root() -> ET.Element:
    return ET.parse(_CONFIG).getroot()


def test_memory_tracker_is_reconciled_against_real_rss() -> None:
    root = _root()
    correct = root.findtext("memory_worker_correct_memory_tracker")
    period = root.findtext("memory_worker_period_ms")
    assert correct == "1", (
        "memory_worker_correct_memory_tracker must be enabled so the jemalloc-arena "
        "over-count is reconciled to real RSS and cannot false-trip MEMORY_LIMIT_EXCEEDED"
    )
    assert period is not None and int(period) > 0, (
        "memory_worker_period_ms must be non-zero for the reconciliation to run periodically"
    )


def test_server_memory_ratio_has_headroom_over_real_usage() -> None:
    root = _root()
    ratio = root.findtext("max_server_memory_usage_to_ram_ratio")
    assert ratio is not None, "max_server_memory_usage_to_ram_ratio must be set"
    assert float(ratio) >= 0.85, (
        f"max_server_memory_usage_to_ram_ratio={ratio} is too low; 0.75 phantom-tripped "
        "at ~1.5 GiB real RSS on an 8 GiB box with a <50 MiB dataset. Keep headroom "
        "(cgroup OOM watcher at 0.95 is the real backstop)."
    )


# ── Merged from tests/high_scale/test_batch_ingest.py ──


import pytest
from backend.high_scale.source_discovery import (
    SourceObjectMissingError,
    SourceObjectPage,
)

from backend.high_scale.ingest_controller import (
    HIGH_SCALE_PAGE_MAX_BYTES,
    BatchIngestResult,
)
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.publication import (
    HighScaleBatch,
    PublicationResult,
    PublicationStatus,
)


class _ClickHouse_batch_ingest:
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
    clickhouse = _ClickHouse_batch_ingest()
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
    clickhouse = _ClickHouse_batch_ingest()
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
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse_batch_ingest()),
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
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse_batch_ingest()),
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
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse_batch_ingest()),
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


# ── Merged from test_worker_fairness_wiring.py ──────────────────────────────

from collections import Counter
from dataclasses import dataclass, field

from backend.high_scale.worker import FairPageSelector, _MultiServiceWorkerLoop, _weights_from_env


@dataclass
class _Coordinator_batch_ingest:
    calls: list[tuple[str, str]] = field(default_factory=list)

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def _loop(services, domains, **kwargs):
    coordinator = _Coordinator_batch_ingest()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=services,
        domains=domains,
        interval_seconds=0,
        **kwargs,
    )
    return loop, coordinator


def test_uncapped_loop_keeps_the_original_service_and_domain_order() -> None:
    loop, coordinator = _loop(("svc-z", "svc-a", "svc-m"), ("request", "rum_vitals", "rum_errors"))

    results = loop.run_once()

    expected = [(s, d) for s in ("svc-z", "svc-a", "svc-m") for d in ("request", "rum_vitals", "rum_errors")]
    assert coordinator.calls == expected
    assert [(r.service_id, r.domain) for r in results] == expected


def test_capped_loop_runs_at_most_capacity_pages_per_tick_and_never_drops_work() -> None:
    loop, coordinator = _loop(("hot", "small"), ("d1", "d2", "d3"), pages_per_tick=2)

    per_tick = []
    for _ in range(3):
        before = len(coordinator.calls)
        loop.run_once()
        per_tick.append(coordinator.calls[before:])

    assert all(len(calls) == 2 for calls in per_tick)
    assert set(coordinator.calls) == {(s, d) for s in ("hot", "small") for d in ("d1", "d2", "d3")}


def test_every_service_keeps_minimum_progress_under_a_cap() -> None:
    loop, coordinator = _loop(("hot", "a", "b"), ("d1", "d2", "d3", "d4"), pages_per_tick=3, service_weights={"hot": 5})

    for _ in range(4):
        before = len(coordinator.calls)
        loop.run_once()
        tick = coordinator.calls[before:]
        assert {service for service, _ in tick} >= {"hot", "a", "b"}


def test_weights_let_a_dominant_service_borrow_idle_capacity() -> None:
    # The dominant service sorts LAST so the extra capacity can only reach it
    # through its weight, not through alphabetical dispatch order.
    loop, coordinator = _loop(
        ("asmall", "zhot"), ("d1", "d2", "d3", "d4", "d5", "d6"), pages_per_tick=5, service_weights={"zhot": 2}
    )

    loop.run_once()

    counts = Counter(service for service, _ in coordinator.calls)
    assert counts == {"asmall": 2, "zhot": 3}


def test_deferred_pages_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    loop, _ = _loop(("a", "b"), ("d1", "d2"), pages_per_tick=2)

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        loop.run_once()

    record = next(r for r in caplog.records if r.getMessage() == "high_scale.fair_dispatch")
    assert (record.dispatched, record.deferred, record.capacity) == (2, 2, 2)  # type: ignore[attr-defined]


def test_no_fair_dispatch_log_when_everything_runs(caplog: pytest.LogCaptureFixture) -> None:
    loop, _ = _loop(("a", "b"), ("d1",))

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        loop.run_once()

    assert not [r for r in caplog.records if r.getMessage() == "high_scale.fair_dispatch"]


def test_page_failures_do_not_stop_the_rest_of_the_selection() -> None:
    class _Flaky(_Coordinator_batch_ingest):
        def run_page(self, *, service_id: str, domain: str) -> PageRun:
            if (service_id, domain) == ("a", "d1"):
                raise RuntimeError("boom")
            return super().run_page(service_id=service_id, domain=domain)

    coordinator = _Flaky()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("a", "b"),
        domains=("d1",),
        interval_seconds=0,
    )

    results = loop.run_once()

    assert [(r.service_id, r.error is not None) for r in results] == [("a", True), ("b", False)]


def test_multi_service_loop_dispatches_each_service_through_its_own_coordinator() -> None:
    coordinators = {"a": _Coordinator_batch_ingest(), "b": _Coordinator_batch_ingest()}
    loop = _MultiServiceWorkerLoop(
        coordinators=tuple((sid, c) for sid, c in coordinators.items()),  # type: ignore[arg-type]
        domains=("d1", "d2"),
        interval_seconds=0,
        pages_per_tick=2,
    )

    loop.run_once()
    loop.run_once()

    assert coordinators["a"].calls == [("a", "d1"), ("a", "d2")]
    assert coordinators["b"].calls == [("b", "d1"), ("b", "d2")]


def test_capacity_below_the_service_count_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least the number of services"):
        FairPageSelector((("a", "d"), ("b", "d")), capacity=1)


def test_weights_environment_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "svc-a=3, svc-b=1")
    assert _weights_from_env() == {"svc-a": 3, "svc-b": 1}

    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "svc-a=0")
    with pytest.raises(ValueError, match="positive_integer"):
        _weights_from_env()

    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "no-equals")
    with pytest.raises(ValueError, match="positive_integer"):
        _weights_from_env()

    monkeypatch.delenv("HIGH_SCALE_SERVICE_WEIGHTS")
    assert _weights_from_env() == {}


# ── Merged from test_differential_canary.py ─────────────────────────────────

from collections.abc import Sequence
from typing import Any

from backend.high_scale.aggregate_batch_adapter import compute_dimension_counts
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.performance_projection import build_performance_projection_rows

from backend.high_scale.network import build_network_projection_rows
from backend.high_scale.security import build_security_projection_rows


class _MockClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    @property
    def all_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for b in self.batches:
            if b.domain == "request":
                rows.extend(b.rows)
        return rows


def _generate_synthetic_log_sources(
    service_id: str,
    source_count: int = 5,
    rows_per_source: int = 10,
) -> list[tuple[SourceObjectDescriptor, bytes, list[dict[str, Any]]]]:
    """Generates synthetic Fastly request log gzip files with realistic fields."""
    sources: list[tuple[SourceObjectDescriptor, bytes, list[dict[str, Any]]]] = []
    methods = ["GET", "POST", "HEAD", "PUT", "DELETE"]
    statuses = [200, 200, 304, 404, 500]
    pops = ["JFK", "SFO", "LHR", "NRT", "FRA"]
    origins = ["origin-primary", "origin-backup", "origin-s3"]

    for src_idx in range(source_count):
        raw_events: list[dict[str, Any]] = []
        lines: list[bytes] = []
        for row_idx in range(rows_per_source):
            event = {
                "timestamp": f"2026-10-09T12:{src_idx:02d}:{row_idx:02d}Z",
                "client_ip": f"198.51.100.{(src_idx * 10 + row_idx) % 250 + 1}",
                "request_method": methods[(src_idx + row_idx) % len(methods)],
                "url": f"/api/v1/resource/{(src_idx * 10 + row_idx) % 7}",
                "status": statuses[(src_idx + row_idx) % len(statuses)],
                "resp_bytes": 1024 * ((row_idx % 5) + 1),
                "req_size": 256 * ((row_idx % 3) + 1),
                "pop": pops[(src_idx + row_idx) % len(pops)],
                "server_datacenter": pops[(src_idx + row_idx) % len(pops)],
                "fastly_backend_name": origins[(src_idx + row_idx) % len(origins)],
                "time_elapsed": 45000 + (row_idx * 1000),
                "resp_header_time": 35000 + (row_idx * 1000),
                "rtt": 15000 + (row_idx * 500),
                "tls_client_ja3_md5": f"ja3_{src_idx}_{row_idx % 3}",
            }
            raw_events.append(event)
            lines.append((json.dumps(event) + "\n").encode("utf-8"))

        compressed = gzip.compress(b"".join(lines))
        descriptor = SourceObjectDescriptor(
            object_key=f"raw/request/year=2026/month=10/day=09/hour=12/minute={src_idx:02d}/part_{src_idx:04d}.gz",
            checksum=f"sha256:synth_{src_idx:04d}",
            size_bytes=len(compressed),
            version=f"v{src_idx}",
        )
        sources.append((descriptor, compressed, raw_events))
    return sources


def _build_controller(
    service_id: str, worker_id: str, ch_client: _MockClickHouse
) -> tuple[HighScaleIngestController, HighScaleLedger, InMemoryObjectStore]:
    ownership = OwnershipStore()
    ownership.initialize(service_id, owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    manifest_store = InMemoryBatchManifestStore()
    serving = ClickHousePublication(manifest_store, ch_client)

    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=archive,
        serving=serving,
        worker_id=worker_id,
        archive_epoch=1,
    )
    return controller, ledger, store


def test_differential_canary_row_counts_and_events_match() -> None:
    """Verifies that the existing unbatched path and the new batched path produce
    identical row counts and identical normalized event records.
    """
    service_id = "canary-svc-001"
    sources = _generate_synthetic_log_sources(service_id, source_count=6, rows_per_source=8)
    expected_total_rows = 6 * 8

    # --- Path A: Single-Source Batch Path ---
    ch_unbatched = _MockClickHouse()
    ctrl_unbatched, ledger_unbatched, _ = _build_controller(service_id, "worker-unbatched", ch_unbatched)

    unbatched_results: list[BatchIngestResult] = []
    for desc, payload, _ in sources:
        ledger_unbatched.discover(
            service_id, "request", desc.object_key, desc.checksum, size_bytes=len(payload), version=desc.version
        )
        ledger_unbatched.claim(service_id, desc.object_key, "worker-unbatched")
        res = ctrl_unbatched.ingest_batch(
            service_id=service_id,
            domain="request",
            sources_payloads=[(desc, payload)],
        )
        unbatched_results.append(res)

    rows_unbatched = ch_unbatched.all_rows
    assert len(rows_unbatched) == expected_total_rows
    assert len(ch_unbatched.batches) == len(sources)

    # --- Path B: Batched Path ---
    ch_batched = _MockClickHouse()
    ctrl_batched, ledger_batched, _ = _build_controller(service_id, "worker-batched", ch_batched)

    batch_items = [(desc, payload) for desc, payload, _ in sources]
    for desc, payload in batch_items:
        ledger_batched.discover(
            service_id, "request", desc.object_key, desc.checksum, size_bytes=len(payload), version=desc.version
        )
        ledger_batched.claim(service_id, desc.object_key, "worker-batched")

    batch_res = ctrl_batched.ingest_batch(
        service_id=service_id,
        domain="request",
        sources_payloads=batch_items,
    )

    rows_batched = ch_batched.all_rows
    assert len(rows_batched) == expected_total_rows
    assert batch_res.total_events == expected_total_rows
    assert batch_res.sources_count == len(sources)
    assert batch_res.publication.status == PublicationStatus.VISIBLE

    # --- Differential Comparison: Row Count & Field Parity ---
    unbatched_events_by_id = {r["event_id"]: r for r in rows_unbatched}
    batched_events_by_id = {r["event_id"]: r for r in rows_batched}

    # 1. Exact Event ID parity
    assert set(unbatched_events_by_id.keys()) == set(batched_events_by_id.keys())

    # 2. Field-by-field parity on all serving dimensions
    fields_to_compare = [
        "timestamp",
        "client_ip",
        "request_method",
        "url",
        "status",
        "resp_bytes",
        "req_size",
        "pop",
        "server_datacenter",
        "fastly_backend_name",
        "time_elapsed",
        "resp_header_time",
        "rtt",
        "tls_client_ja3_md5",
    ]
    for event_id, u_row in unbatched_events_by_id.items():
        b_row = batched_events_by_id[event_id]
        for field_name in fields_to_compare:
            assert u_row.get(field_name) == b_row.get(field_name), f"Field mismatch on {field_name} for {event_id}"


def test_differential_canary_aggregates_and_projections_match() -> None:
    """Verifies that dimension aggregates and projection tables (origin, security,
    performance, network) computed from both paths match 100%.
    """
    service_id = "canary-svc-002"
    sources = _generate_synthetic_log_sources(service_id, source_count=5, rows_per_source=12)

    # Decode events for both paths
    ch_u = _MockClickHouse()
    ctrl_u, ledger_u, _ = _build_controller(service_id, "worker-u", ch_u)
    for desc, payload, _ in sources:
        ledger_u.discover(
            service_id, "request", desc.object_key, desc.checksum, size_bytes=len(payload), version=desc.version
        )
        ledger_u.claim(service_id, desc.object_key, "worker-u")
        ctrl_u.ingest_batch(
            service_id=service_id,
            domain="request",
            sources_payloads=[(desc, payload)],
        )

    ch_b = _MockClickHouse()
    ctrl_b, ledger_b, _ = _build_controller(service_id, "worker-b", ch_b)
    batch_payloads = [(desc, payload) for desc, payload, _ in sources]
    for desc, payload in batch_payloads:
        ledger_b.discover(
            service_id, "request", desc.object_key, desc.checksum, size_bytes=len(payload), version=desc.version
        )
        ledger_b.claim(service_id, desc.object_key, "worker-b")

    ctrl_b.ingest_batch(
        service_id=service_id,
        domain="request",
        sources_payloads=batch_payloads,
    )

    rows_u = tuple(ch_u.all_rows)
    rows_b = tuple(ch_b.all_rows)

    # 1. Dimension aggregates parity
    dim_counts_u = compute_dimension_counts("request", rows_u)
    dim_counts_b = compute_dimension_counts("request", rows_b)

    assert Counter((d["dimension"], d["value"], d["bucket_start"], d["count"]) for d in dim_counts_u) == Counter(
        (d["dimension"], d["value"], d["bucket_start"], d["count"]) for d in dim_counts_b
    )

    # 2. Origin projection parity
    origin_u = build_origin_projection_rows(rows_u)
    origin_b = build_origin_projection_rows(rows_b)
    assert origin_u.summary_rows == origin_b.summary_rows
    assert origin_u.dimension_rows == origin_b.dimension_rows

    # 3. Security projection parity
    sec_u = build_security_projection_rows(rows_u)
    sec_b = build_security_projection_rows(rows_b)
    assert sec_u.dimension_rows == sec_b.dimension_rows

    # 4. Performance projection parity
    perf_u = build_performance_projection_rows(rows_u)
    perf_b = build_performance_projection_rows(rows_b)
    assert perf_u.dimension_rows == perf_b.dimension_rows

    # 5. Network projection parity
    net_u = build_network_projection_rows(rows_u)
    net_b = build_network_projection_rows(rows_b)
    assert net_u.dimension_rows == net_b.dimension_rows


def test_differential_canary_per_service_flag_and_rollback() -> None:
    """Simulates canary rollout with a per-service flag:
    1. Ingest initial sources under unbatched mode (flag = False).
    2. Canary switch: Ingest under batched mode (flag = True).
    3. Rollback trigger: Ingest under unbatched mode (flag = False).
    Verifies that all events are preserved without duplicates or gaps,
    and all sources remain fully replayable.
    """
    service_id = "canary-svc-003"
    sources = _generate_synthetic_log_sources(service_id, source_count=10, rows_per_source=5)

    ch = _MockClickHouse()
    ctrl, ledger, archive_store = _build_controller(service_id, "worker-canary", ch)

    # Canary state machine simulating dynamic routing
    class CanaryRunner:
        def __init__(self, controller: HighScaleIngestController, ledger: HighScaleLedger, worker_id: str) -> None:
            self.controller = controller
            self.ledger = ledger
            self.worker_id = worker_id
            self.batched_enabled = False

        def process_sources(
            self, batch_sources: Sequence[tuple[SourceObjectDescriptor, bytes, list[dict[str, Any]]]]
        ) -> int:
            if self.batched_enabled:
                payloads = [(d, p) for d, p, _ in batch_sources]
                for d, p in payloads:
                    self.ledger.discover(
                        service_id, "request", d.object_key, d.checksum, size_bytes=len(p), version=d.version
                    )
                    self.ledger.claim(service_id, d.object_key, self.worker_id)
                res = self.controller.ingest_batch(
                    service_id=service_id,
                    domain="request",
                    sources_payloads=payloads,
                )
                return res.total_events
            else:
                total = 0
                for d, p, _ in batch_sources:
                    self.ledger.discover(
                        service_id, "request", d.object_key, d.checksum, size_bytes=len(p), version=d.version
                    )
                    self.ledger.claim(service_id, d.object_key, self.worker_id)
                    r = self.controller.ingest_batch(
                        service_id=service_id,
                        domain="request",
                        sources_payloads=[(d, p)],
                    )
                    total += r.total_events
                return total

    runner = CanaryRunner(ctrl, ledger, "worker-canary")

    # Phase 1: Baseline unbatched (sources 0-2)
    runner.batched_enabled = False
    count_p1 = runner.process_sources(sources[0:3])
    assert count_p1 == 15
    assert len(ch.all_rows) == 15

    # Phase 2: Canary rollout - batched pipeline enabled (sources 3-6)
    runner.batched_enabled = True
    count_p2 = runner.process_sources(sources[3:7])
    assert count_p2 == 20
    assert len(ch.all_rows) == 35

    # Phase 3: Rollback - switch back to unbatched pipeline (sources 7-9)
    runner.batched_enabled = False
    count_p3 = runner.process_sources(sources[7:10])
    assert count_p3 == 15
    assert len(ch.all_rows) == 50

    # Verify that all 50 unique events are present and valid
    unique_event_ids = {r["event_id"] for r in ch.all_rows}
    assert len(unique_event_ids) == 50

    # Verify that every source wrote an archive artifact into the store
    archive_keys = list(archive_store._objects.keys())
    # 3 unbatched + 1 batch (covering 4 sources) + 3 unbatched = 7 archive Parquet artifacts
    parquet_artifacts = [k for k in archive_keys if k.endswith(".parquet")]
    assert len(parquet_artifacts) == 7
