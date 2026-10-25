from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.high_scale.replay import replay_manifest
from backend.high_scale.source_discovery import SourceObjectDescriptor

from backend.high_scale.archive_models import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
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

from backend.high_scale.orchestration import PageRun
from backend.high_scale.worker import HighScaleWorkerLoop


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

from backend.high_scale import worker
from backend.high_scale.archive_models import HighScaleConfig, TopologyProfile, from_environment


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
