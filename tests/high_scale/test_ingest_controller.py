from __future__ import annotations

import gzip
import json
import logging
from collections import Counter
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.high_scale.archive import (
    ArchivePublication,
    ArchiveState,
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InMemoryObjectStore,
    InsertReceipt,
    replay_manifest,
)
from backend.high_scale.ingest_controller import HighScaleIngestController, SourceObjectDescriptor
from backend.high_scale.postgres_control import DeletionController
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

from backend.high_scale.archive import (
    HighScaleBatch,
    PublicationResult,
    PublicationStatus,
)
from backend.high_scale.ingest_controller import (
    HIGH_SCALE_PAGE_MAX_BYTES,
    BatchIngestResult,
    HighScaleWorkerCoordinator,
    SourceObjectMissingError,
    SourceObjectPage,
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


# ── Merged from test_differential_canary.py ─────────────────────────────────

from collections.abc import Sequence
from typing import Any

from backend.high_scale.ingest_controller import compute_dimension_counts
from backend.high_scale.network import build_network_projection_rows
from backend.high_scale.query_service import build_origin_projection_rows, build_performance_projection_rows
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


# ── Merged from tests/high_scale/test_aggregates.py ──

import builtins
import math
import random
import re
import sys as _sys
from types import SimpleNamespace
from uuid import UUID

import pytest

import backend.high_scale.archive as publication
from backend.core.request_telemetry import Attribution, current_attribution, current_clickhouse_scope
from backend.high_scale import _common as raw_query
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import dashboard as hs_dashboard
from backend.high_scale import security as hs
from backend.high_scale.archive import (
    PartialInsert,
    ServingWatermark,
    _batch_uuid,
    _rows_for_domain,
    build_batch_id_from_manifest_id,
    build_cmcd_projection_key,
)
from backend.high_scale.archive import _extract_custom_fields as persisted_fact_fields
from backend.high_scale.ingest_controller import (
    RELATIVE_ERROR,
    AggregateBatchAdapter,
    HighScaleService,
    _enrich_high_scale_ngwaf_bots,
    bucket,
    finite_float,
    percentiles,
    range_value,
    rebuild_security_projections_from_fos,
    replay_security_projections,
    sketch,
    text,
)
from backend.high_scale.query_service import (
    AggregateRequest,
    cmcd_aggregates,
    project_cmcd,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.models.analytics import CmcdAggregatesResponse, CmcdRequest
from backend.utils import telemetry

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, responses: list[list[dict[str, object]]]) -> None:
        self.responses = list(responses)
        self.sql = ""
        self.params: dict[str, object] | None = None
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.calls.append((sql, params))
        self.sql, self.params = sql, params
        return self.responses.pop(0)


def watermark(domain: str = "request") -> ServingWatermark:
    return ServingWatermark(
        "svc", domain, 1, NOW - timedelta(hours=2), NOW - timedelta(minutes=1), "cursor", "archived", "visible", True
    )


def test_query_reads_visible_rows_with_bound_values() -> None:
    client = FakeClient(
        [[{"total_count": 9}], [{"value": "/a", "aggregate_count": 3}, {"value": "/b", "aggregate_count": 2}]]
    )
    req = AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW)
    response = query_clickhouse_aggregate(client, req, watermark=watermark(), now=NOW)
    assert response.request_count == 9 and response.top_values == (("/a", 3), ("/b", 2))
    assert "request_aggregates" in client.sql and "publication_state='visible'" in client.sql
    assert "LIMIT 10" in client.sql and client.params is not None and client.params["service_id"] == "svc"
    assert len(client.calls) == 2 and "OVER ()" not in client.calls[1][0]


@pytest.mark.parametrize(
    ("domain", "table", "metric"),
    [
        ("rum_vitals", "rum_vitals_aggregates", "event_count"),
        ("rum_errors", "rum_error_aggregates", "error_count"),
        ("cmcd", "cmcd_aggregates", "event_count"),
    ],
)
def test_domain_tables_are_isolated(domain: str, table: str, metric: str) -> None:
    client = FakeClient([[], []])
    query_clickhouse_aggregate(client, AggregateRequest("svc", domain), watermark=watermark(domain), now=NOW)
    assert table in client.sql and f"sum({metric})" in client.sql


def test_range_parameters_use_a_clickhouse_type_matching_the_parameter_format() -> None:
    client = FakeClient([[], []])
    req = AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW)
    query_clickhouse_aggregate(client, req, watermark=watermark(), now=NOW)
    assert "{start:DateTime64(3)}" in client.sql and "{end:DateTime64(3)}" in client.sql
    assert "bucket_start < {end:DateTime64(3)}" in client.sql
    assert "{start:DateTime}" not in client.sql and "{end:DateTime}" not in client.sql


def test_partial_minute_query_reads_event_time_facts_with_visibility_fence() -> None:
    start, end = NOW + timedelta(seconds=10), NOW + timedelta(minutes=1, seconds=20)
    client = FakeClient([[{"total_count": 3}], [{"value": "/a", "aggregate_count": 2}]])
    response = query_clickhouse_aggregate(
        client, AggregateRequest("svc", "request", start, end), watermark=watermark(), now=NOW
    )
    assert response.request_count == 3 and response.top_values == (("/a", 2),)
    assert all("request_facts" in sql and "request_aggregates" not in sql for sql, _ in client.calls)
    assert all("event_timestamp >= {start:DateTime64(3)}" in sql for sql, _ in client.calls)
    assert all("event_timestamp < {end:DateTime64(3)}" in sql for sql, _ in client.calls)
    assert all("publication_state='visible'" in sql for sql, _ in client.calls)
    assert client.calls[0][1] is not None and client.calls[0][1]["start"] == start and client.calls[0][1]["end"] == end


def test_partial_minute_batch_reads_fact_dimensions_and_preserves_domain_bounds() -> None:
    start, end = NOW + timedelta(seconds=10), NOW + timedelta(minutes=1, seconds=20)
    client = FakeClient(
        [
            [{"dimension": "url", "total_count": 3}, {"dimension": "country", "total_count": 3}],
            [
                {"dimension": "url", "value": "/a", "aggregate_count": 2},
                {"dimension": "country", "value": "US", "aggregate_count": 3},
            ],
        ]
    )
    responses = query_clickhouse_aggregates_batch(
        client,
        service_id="svc",
        domain="request",
        dimensions=["url", "country"],
        start=start,
        end=end,
        watermark=watermark(),
        now=NOW,
    )
    assert responses["url"].request_count == 3 and responses["country"].top_values == (("US", 3),)
    assert len(client.calls) == 2
    assert all("request_facts" in sql and "publication_state='visible'" in sql for sql, _ in client.calls)
    assert all("event_timestamp < {end:DateTime64(3)}" in sql for sql, _ in client.calls)
    assert "tuple({fact_dimension_0:String}, toString(url))" in client.calls[0][0]
    assert "tuple({fact_dimension_1:String}, toString(country))" in client.calls[0][0]


def test_partial_minute_cmcd_session_reads_the_fact_map() -> None:
    client = FakeClient([[{"total_count": 2}], [{"value": "session-1", "aggregate_count": 2}]])
    req = AggregateRequest("svc", "cmcd", NOW + timedelta(seconds=1), NOW + timedelta(minutes=1))
    response = query_clickhouse_aggregate(client, req, watermark=watermark("cmcd"), now=NOW)
    assert response.top_values == (("session-1", 2),)
    assert all("cmcd_projection_facts" in sql and "cmcd['sid']" in sql for sql, _ in client.calls)
    assert all("event_timestamp <= {end:DateTime64(3)}" in sql and "domain='cmcd'" in sql for sql, _ in client.calls)


@pytest.mark.parametrize(
    ("domain", "table", "expression"),
    [
        ("rum_vitals", "rum_vitals_facts", "toString(metric_name)"),
        ("rum_errors", "rum_error_facts", "toString(error_message)"),
    ],
)
def test_partial_minute_rum_reads_event_time_facts_with_inclusive_end(domain: str, table: str, expression: str) -> None:
    client = FakeClient([[{"total_count": 1}], [{"value": "event", "aggregate_count": 1}]])
    req = AggregateRequest("svc", domain, NOW + timedelta(seconds=1), NOW + timedelta(minutes=1))
    query_clickhouse_aggregate(client, req, watermark=watermark(domain), now=NOW)
    assert all(table in sql and expression in sql for sql, _ in client.calls)
    assert all(
        "event_timestamp <= {end:DateTime64(3)}" in sql and f"domain='{domain}'" in sql for sql, _ in client.calls
    )


def test_partial_minute_request_cmcd_session_uses_request_schema_map() -> None:
    client = FakeClient([[{"total_count": 1}], [{"value": "session-1", "aggregate_count": 1}]])
    req = AggregateRequest("svc", "request", NOW + timedelta(seconds=1), NOW + timedelta(minutes=1), "cmcd_session")
    response = query_clickhouse_aggregate(client, req, watermark=watermark(), now=NOW)
    assert response.top_values == (("session-1", 1),)
    assert all("request_facts" in sql and "cmcd['sid']" in sql for sql, _ in client.calls)


def test_partial_minute_query_rejects_dimensions_without_fact_mapping() -> None:
    req = AggregateRequest("svc", "request", NOW + timedelta(seconds=1), NOW + timedelta(minutes=1), "made_up")
    with pytest.raises(ValueError, match="cannot be read exactly"):
        query_clickhouse_aggregate(FakeClient([[], []]), req, watermark=watermark(), now=NOW)


def test_query_allows_digit_suffix_in_internal_dimension_identifier() -> None:
    client = FakeClient([[], []])
    query_clickhouse_aggregate(
        client, AggregateRequest("svc", "request", dimension="is_ipv6"), watermark=watermark(), now=NOW
    )
    assert client.params is not None and client.params["dimension"] == "is_ipv6"


@pytest.mark.parametrize("dimension", ["3xx", "ja-3", "url; DROP TABLE request_aggregates"])
def test_query_rejects_non_identifier_dimension(dimension: str) -> None:
    with pytest.raises(ValueError, match="internal identifier"):
        query_clickhouse_aggregate(
            FakeClient([[], []]),
            AggregateRequest("svc", "request", dimension=dimension),
            watermark=watermark(),
            now=NOW,
        )


def test_watermark_must_match_request_domain() -> None:
    with pytest.raises(ValueError, match="watermark"):
        query_clickhouse_aggregate(
            FakeClient([[], []]), AggregateRequest("svc", "request"), watermark=watermark("cmcd")
        )


# ── Merged from test_aggregate_writer.py ──


def test_computes_counts_per_dimension_value_and_minute_bucket() -> None:
    rows = (
        {"timestamp": "2026-09-14T00:00:10Z", "url": "/a", "country": "US", "ip": "1.1.1.1"},
        {"timestamp": "2026-09-14T00:00:40Z", "url": "/a", "country": "US", "ip": "1.1.1.2"},
        {"timestamp": "2026-09-14T00:01:05Z", "url": "/b", "country": "CA", "ip": "1.1.1.1"},
    )
    by_key = {
        (row["dimension"], row["value"], row["bucket_start"]): row["count"]
        for row in compute_dimension_counts("request", rows)
    }
    m0 = datetime(2026, 9, 14, 0, 0, tzinfo=UTC).isoformat()
    m1 = datetime(2026, 9, 14, 0, 1, tzinfo=UTC).isoformat()
    assert by_key[("url", "/a", m0)] == 2 and by_key[("url", "/b", m1)] == 1
    assert by_key[("country", "US", m0)] == 2 and by_key[("country", "CA", m1)] == 1
    assert by_key[("client_ip", "1.1.1.1", m0)] == 1 and by_key[("client_ip", "1.1.1.2", m0)] == 1


def test_request_counts_include_all_dashboard_card_dimensions() -> None:
    row = {
        "timestamp": "2026-09-14T00:00:10Z",
        "url": "/a",
        "country": "US",
        "ip": "1.1.1.1",
        "host": "logs.example.test",
        "method": "GET",
        "status": 200,
        "cache": "HIT",
        "proto": "HTTP/3",
        "ua": "Synthetic Browser",
        "referer": "https://example.test/",
        "city": "Seattle",
        "region": "WA",
        "pop": "SEA",
        "backend": "origin-a",
        "tls": "1.3",
    }
    values = {(r["dimension"], r["value"]) for r in compute_dimension_counts("request", (row,))}
    expected = {
        ("host", "logs.example.test"),
        ("method", "GET"),
        ("status", "200"),
        ("cache", "HIT"),
        ("proto", "HTTP/3"),
        ("ua", "Synthetic Browser"),
        ("referer", "https://example.test/"),
        ("city", "Seattle"),
        ("region", "WA"),
        ("pop", "SEA"),
        ("backend", "origin-a"),
        ("tls", "1.3"),
    }
    assert expected <= values


def test_rum_vitals_uses_metric_name_dimension() -> None:
    counts = compute_dimension_counts("rum_vitals", ({"timestamp": "2026-09-14T00:00:00Z", "metric_name": "LCP"},))
    assert len(counts) == 1 and counts[0]["dimension"] == "metric_name" and counts[0]["value"] == "LCP"


def test_cmcd_dimension_reads_the_session_id_field() -> None:
    counts = compute_dimension_counts("cmcd", ({"timestamp": "2026-09-14T00:00:00Z", "sid": "session-1"},))
    assert counts[0]["dimension"] == "cmcd_session" and counts[0]["value"] == "session-1"


def test_rows_without_a_timestamp_are_skipped() -> None:
    assert compute_dimension_counts("request", ({"url": "/a"},)) == ()


def test_unsupported_domain_is_rejected() -> None:
    with pytest.raises(ValueError, match="domain"):
        compute_dimension_counts("bogus", ())


# ── Merged from test_projections.py ──


def test_cmcd_projection_preserves_request_identity_and_full_session_id() -> None:
    request = {
        "event_id": "request-event-1",
        "service_id": "svc",
        "timestamp": "2026-09-01T00:00:00Z",
        "cmcd_version": "v2",
        "sid": "session-" + "x" * 64,
        "cmcd": {"br": 1200, "d": 4000},
    }
    projection = project_cmcd(request)
    assert (
        projection is not None and projection["request_event_id"] == "request-event-1" and projection["projection_key"]
    )
    assert projection["sid"] == request["sid"] and projection["cmcd_version"] == "v2"
    assert projection["fields"] == {"br": 1200, "d": 4000}


def test_cmcd_projection_is_absent_when_request_has_no_cmcd() -> None:
    assert project_cmcd({"event_id": "request-event-1", "service_id": "svc"}) is None


def test_cmcd_projection_requires_request_event_identity() -> None:
    with pytest.raises(ValueError, match="request_event_id"):
        project_cmcd({"service_id": "svc", "cmcd": {"br": 1200}})


def test_cmcd_projection_carries_the_request_geography_the_analytics_group_by() -> None:
    projection = project_cmcd(
        {
            "event_id": "request-event-1",
            "service_id": "svc",
            "timestamp": "2026-09-01T00:00:00Z",
            "country": "US",
            "asn": 7922,
            "cmcd": {"br": "1200"},
        }
    )
    assert projection is not None and projection["country"] == "US" and projection["asn"] == 7922


@pytest.mark.parametrize(
    ("raw_asn", "expected"), [("15169", 15169), ("", None), ("AS15169", None), (None, None), (-4, None)]
)
def test_cmcd_projection_normalises_asn_or_drops_it(raw_asn, expected) -> None:
    projection = project_cmcd(
        {"event_id": "e", "service_id": "svc", "timestamp": "2026-09-01T00:00:00Z", "asn": raw_asn, "cmcd": {"br": "1"}}
    )
    assert projection is not None and projection["asn"] == expected


def test_cmcd_projection_defaults_missing_country_to_empty() -> None:
    projection = project_cmcd(
        {"event_id": "e", "service_id": "svc", "timestamp": "2026-09-01T00:00:00Z", "cmcd": {"br": "1"}}
    )
    assert projection is not None and projection["country"] == ""


# ── Merged from test_projection_utils.py ──


def test_range_value_keeps_naive_for_date_only_and_tz_for_z():
    assert range_value(None) is None
    assert range_value("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert range_value("2026-01-02").tzinfo is None


def test_bucket_floors_to_utc_minute_and_rejects_garbage():
    assert bucket("2026-01-02T03:04:59.9Z") == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket(datetime(2026, 1, 2, 3, 4, 59, 5, tzinfo=UTC)) == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket("not a date") is None and bucket(5) is None


def test_text_and_finite_float():
    assert text("  x ") == "x" and text(5) == "" and finite_float("1.5") == 1.5
    for invalid in (True, "", "nan", "inf"):
        assert finite_float(invalid) is None


@pytest.mark.parametrize(
    "values,q,expected",
    [([], 0.5, None), ([4.0], 0.99, 4.0), ([1.0, 2.0, 3.0, 4.0], 0.5, 2.5), ([10.0, 0.0], 0.95, pytest.approx(9.5))],
)
def test_percentiles_linear_interpolation(values, q, expected):
    assert percentiles(values, (q,)) == [expected]


def test_percentiles_sorts_once(monkeypatch):
    calls = []
    real_sorted = builtins.sorted
    monkeypatch.setattr(raw_query, "sorted", lambda *a, **k: calls.append(1) or real_sorted(*a, **k), raising=False)
    raw_query.percentiles([3.0, 1.0, 2.0], (0.5, 0.95, 0.99))
    assert len(calls) == 1


# ── Merged from test_iso_parse_pins.py ──


def test_publication_timestamp_assumes_utc_for_naive_and_wraps_errors() -> None:
    assert publication._timestamp({"t": "2026-01-02"}, "t") == datetime(2026, 1, 2, tzinfo=UTC)
    assert publication._timestamp({"t": "2026-01-02T03:04:05Z"}, "t") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="t must be an ISO timestamp"):
        publication._timestamp({"t": "garbage"}, "t")


@pytest.mark.parametrize("fn,key", [(raw_query._row_timestamp, "timestamp")])
def test_astimezone_family_returns_utc_aware(fn, key) -> None:
    assert fn({key: "2026-01-02T03:04:05Z"}) == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02T03:04:05+02:00"}) == datetime(2026, 1, 2, 1, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02"}).tzinfo is not None and fn({key: 5}) is None
    with pytest.raises(ValueError):
        fn({key: "garbage"})


# ── Merged from test_sketch.py ──


def _weighted_quantile(centroids: list[float], counts: list[int], q: float) -> float:
    target, seen = q * sum(counts), 0
    for centroid, count in zip(centroids, counts, strict=True):
        seen += count
        if seen >= target:
            return centroid
    return centroids[-1]


def test_empty_input_yields_empty_sketch() -> None:
    assert sketch([]) == ([], [])


def test_counts_conserve_every_observation() -> None:
    values = [0.0, 0.0, 5.0, 5.0, 5.1, 1_000.0, -20.0]
    centroids, counts = sketch(values)
    assert sum(counts) == len(values) and centroids == sorted(centroids) and len(centroids) == len(counts)


def test_zero_and_negative_values_keep_their_sign() -> None:
    centroids, counts = sketch([0.0, -100.0, 100.0])
    assert centroids[0] < 0 < centroids[2] and centroids[1] == 0.0 and counts == [1, 1, 1]


def test_nearby_values_share_a_centroid_and_far_values_do_not() -> None:
    centroids, counts = sketch([1000.0, 1000.5, 50_000.0])
    assert counts == [2, 1] and len(centroids) == 2


@pytest.mark.parametrize("q", [0.5, 0.75, 0.95, 0.99])
def test_merged_quantiles_stay_within_the_documented_relative_error(q: float) -> None:
    rng = random.Random(7)
    minutes = [[rng.lognormvariate(8.0, 1.2) for _ in range(300)] for _ in range(20)]
    merged: dict[float, int] = {}
    for minute in minutes:
        centroids, counts = sketch(minute)
        for centroid, count in zip(centroids, counts, strict=True):
            merged[centroid] = merged.get(centroid, 0) + count
    everything = sorted(value for minute in minutes for value in minute)
    exact = everything[math.ceil(q * len(everything)) - 1]
    estimate = _weighted_quantile(sorted(merged), [merged[c] for c in sorted(merged)], q)
    assert estimate == pytest.approx(exact, rel=RELATIVE_ERROR * 1.001)


# ── Merged from test_aggregate_batch_adapter.py ─────────────────────────────


def _batch(domain: str = "request_aggregate") -> HighScaleBatch:
    b = datetime(2026, 9, 14, tzinfo=UTC).isoformat()
    rows = (
        {"dimension": "url", "value": "/a", "bucket_start": b, "count": 3},
        {"dimension": "country", "value": "US", "bucket_start": b, "count": 3},
    )
    return HighScaleBatch(batch_id="batch-1", service_id="svc", domain=domain, generation="epoch-1", rows=rows)


def _origin_batch(domain: str) -> HighScaleBatch:
    row: dict[str, Any] = {
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
        "latency_sk_v": [1000.0, 2000.0],
        "latency_sk_w": [2, 1],
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
                "ttlb_sk_v": [1800.0],
                "ttlb_sk_w": [3],
                "overhead_sk_v": [500.0],
                "overhead_sk_w": [3],
                "origin_bytes_sk_v": [100.0],
                "origin_bytes_sk_w": [3],
            }
        )
    else:
        row.update({"dimension": "url", "value": "/a"})
    return HighScaleBatch(
        batch_id=f"batch-{domain}", service_id="svc", domain=domain, generation="epoch-1", rows=(row,)
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
            self.publications[str(rows[0][0])] = dict(zip(columns, rows[0], strict=True))
        else:
            self.rows.setdefault(str(rows[0][-2]), []).extend(rows)

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        self.rows.pop(str(batch_id), None)


def test_inserts_exact_allowlisted_shape() -> None:
    client = FakeHttpClickHouse()
    result = AggregateBatchAdapter(client).insert(_batch())
    assert result.rows_inserted == 2
    pending, facts, visible = client.inserts
    assert pending[0] == "high_scale_batch_publications" and facts[0] == "request_aggregates"
    expected_cols = [
        "service_id",
        "bucket_start",
        "dimension",
        "value",
        "request_count",
        "batch_id",
        "publication_state",
    ]
    assert facts[1] == expected_cols and UUID(facts[2][0][-2]) and visible[2][0][7:9] == (1, "visible")


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
    assert facts[0] == table and metric_column in facts[1]


def test_duplicate_retry_does_not_insert_twice() -> None:
    client = FakeHttpClickHouse()
    adapter = AggregateBatchAdapter(client)
    batch = _batch()
    assert adapter.insert(batch).rows_inserted == adapter.insert(batch).rows_inserted == 2
    assert len([call for call in client.inserts if call[0] == "request_aggregates"]) == 1


def test_partial_insert_is_cleaned_up_and_raised() -> None:
    client = FakeHttpClickHouse()
    batch = _batch()
    client.rows[_batch_uuid(batch)] = [("stale",)]
    with pytest.raises(PartialInsert):
        AggregateBatchAdapter(client).insert(batch)


def test_rejects_a_domain_outside_the_aggregate_allowlist() -> None:
    with pytest.raises(ValueError, match="domain"):
        AggregateBatchAdapter(FakeHttpClickHouse()).insert(_batch("request"))


@pytest.mark.parametrize(
    ("domain", "table"),
    [("origin_summary", "origin_minute_summary"), ("origin_dimensions", "origin_minute_dimensions")],
)
def test_maps_origin_projection_domains_to_allowlisted_tables(domain, table) -> None:
    client = FakeHttpClickHouse()
    result = AggregateBatchAdapter(client).insert(_origin_batch(domain))
    assert result.rows_inserted == 1
    facts = client.inserts[1]
    assert facts[0] == table and facts[2][0][-2] and {"latency_sk_v", "latency_sk_w"} <= set(facts[1])
    assert ("origin_bytes_sk_v" in facts[1]) == (domain == "origin_summary")
    assert facts[2][0][facts[1].index("latency_sk_w")] == [2, 1]


def _performance_batch(domain: str) -> HighScaleBatch:
    bucket_iso = datetime(2026, 9, 14, tzinfo=UTC).isoformat()
    if domain == "performance_summary":
        row = {
            "bucket_start": bucket_iso,
            "component_count": 2,
            "edge_processing_sum_ms": 1.0,
            "origin_wait_sum_ms": 2.0,
            "origin_download_sum_ms": 3.0,
            "client_download_sum_ms": 4.0,
        }
    else:
        row = {"bucket_start": bucket_iso, "sample_key": 2**63 + 5, "origin_ms": 1.5, "edge_ms": 2.5, "cache": "HIT"}
    return HighScaleBatch(
        batch_id=f"batch-{domain}", service_id="svc", domain=domain, generation="epoch-1", rows=(row,)
    )


@pytest.mark.parametrize(
    ("domain", "table", "column"),
    [
        ("performance_summary", "performance_minute_summary", "component_count"),
        ("performance_scatter", "performance_scatter_samples", "sample_key"),
    ],
)
def test_maps_performance_projection_domains_to_allowlisted_tables(domain, table, column) -> None:
    client = FakeHttpClickHouse()
    result = AggregateBatchAdapter(client).insert(_performance_batch(domain))
    assert result.rows_inserted == 1
    inserted_table, columns, rows = client.inserts[1]
    assert inserted_table == table and column in columns and len(rows[0]) == len(columns)


# ── Merged from tests/high_scale/test_security.py ──

START = "2026-10-18T00:00:00Z"
END = "2026-10-19T00:00:00Z"
_EPOCH = int(datetime(2026, 10, 18, 3, 0, tzinfo=UTC).timestamp())
_TAG = re.compile(r"hs_security:(\w+)")


class _Client:
    def __init__(self, rows: dict[str, list[dict[str, Any]]] | None = None, error: Exception | None = None):
        self.rows, self.error = rows or {}, error
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        match = _TAG.search(sql)
        assert match, f"query is missing its hs_security tag: {sql[:120]}"
        tag = match.group(1)
        self.calls.append((tag, sql, params or {}))
        return self.rows.get(tag, [])

    def tags(self) -> list[str]:
        return [tag for tag, _, _ in self.calls]

    def call(self, tag: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for t, sql, params in self.calls if t == tag)


def _service(client: _Client) -> HighScaleService:
    wm = ServingWatermark("svc", "request", 1, None, None, None, None, None, True)
    return HighScaleService(service_id="svc", client=client, cursor_secret=b"secret", request_watermark=wm)


def _req(filters: dict[str, Any] | None = None, bucket_seconds: int = 3600) -> SimpleNamespace:
    return SimpleNamespace(filters=filters or {}, bucket_seconds=bucket_seconds)


_COUNTRY_US = {"country": {"mode": "include", "values": ["US"]}}
_BOT_ENTRY = {"id": "googlebot", "categories": ["search-engine"], "verification": {"domains": ["googlebot.com"]}}


def _match_googlebot(ua: str | None) -> tuple[dict[str, Any], ...]:
    return (_BOT_ENTRY,) if "Googlebot" in (ua or "") else ()


def test_every_section_uses_the_standard_contract_shapes():
    rows = {
        "tls_fingerprints": [{"fingerprint": "abc", "ip_count": "2", "request_count": "5"}],
        "fingerprint_coverage": [{"total_rows": "10", "populated": "4"}],
        "req_size_dist": [{"bucket": "0-256B", "count": "9", "min_val": "0"}],
        "conn_reuse_dist": [{"bucket": "2-5", "count": "4", "min_val": "2"}],
        "ipv6_adoption": [{"bucket_ts": _EPOCH, "pct": 40.0}],
        "proxy_dist": [{"type": "vpn", "count": "7"}],
        "verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_type": "search-engine", "count": "3"}],
        "top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}],
    }
    res = hs.security_aggregates(_service(_Client(rows)), _req(), START, END, sections=set(rows)).model_dump(
        exclude_unset=True
    )
    assert res["tls_fingerprints"] == [{"fingerprint": "abc", "ip_count": 2, "request_count": 5}]
    assert res["fingerprint_coverage"] == {"tls_ciphers_sha": 0.4}
    assert res["req_size_dist"] == [{"bucket": "0-256B", "count": 9}]
    assert res["conn_reuse_dist"] == [{"bucket": "2-5", "count": 4}]
    assert res["ipv6_adoption"] == [{"time": "2026-10-18T03:00:00Z", "pct": 40.0}]
    assert res["proxy_dist"] == [{"type": "vpn", "count": 7}]
    assert res["verified_bots_ts"] == [{"time": "2026-10-18T03:00:00Z", "bot_type": "search-engine", "count": 3}]
    assert res["top_ips_header"] == [{"ip": "10.0.1.5", "max_header": 9000}]


def test_fingerprint_coverage_is_computed_not_hardcoded():
    client = _Client({"fingerprint_coverage": [{"total_rows": "0", "populated": "0"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"fingerprint_coverage"})
    assert res.fingerprint_coverage == {"tls_ciphers_sha": 0.0}


def test_proxy_and_header_sections_read_the_standard_columns():
    client = _Client()
    hs.security_aggregates(
        _service(client), _req(), START, END, sections={"proxy_dist", "req_size_dist", "ipv6_adoption"}
    )
    assert "custom_fields['p_type']" in client.call("proxy_dist")[0]
    assert "req_header_bytes" in client.call("req_size_dist")[0] and "toStartOfHour" in client.call("ipv6_adoption")[0]


def test_sections_selector_only_runs_requested_queries():
    client = _Client()
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    assert client.tags() == ["proxy_dist"] and res.tls_fingerprints == []


def test_no_sections_runs_every_section_including_bots_and_proxies():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(), START, END)
    expected = {
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
        "ngwaf_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "top_bots_ua",
        "proxy_stats",
        "traffic_quality",
        "suspicious_isps",
        "active_clients",
    }
    assert set(client.tags()) >= expected


def test_request_filters_are_bound_into_every_request_facts_query():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(_COUNTRY_US), START, END)
    assert client.calls
    for tag, sql, params in client.calls:
        assert params["filter_0_0"] == "US" and "country IN ({filter_0_0:String})" in sql, tag


def test_unfiltered_ngwaf_sections_read_the_security_projection():
    client = _Client(
        {
            "ngwaf_bots": [
                {"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"},
                {"bot_name": "other", "wk_name": "", "bot_cat": "", "request_count": "2"},
            ],
            "ngwaf_verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_name": "bingbot", "count": "4"}],
        }
    )
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"):
        res = hs.security_aggregates(
            _service(client), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    summary_sql = client.call("ngwaf_bots")[0]
    assert "FROM security_minute_dimensions" in summary_sql and "dimension='ngwaf_bot'" in summary_sql
    assert "request_facts" not in summary_sql
    assert "FROM security_minute_dimensions" in client.call("ngwaf_verified_bots_ts")[0]
    assert res.ngwaf_configured is True
    assert [r.model_dump() for r in res.ngwaf_verified_bots] == [
        {"bot_name": "bingbot", "wellknown_bot_name": "bing", "category": "search", "request_count": 11},
        {"bot_name": "other", "wellknown_bot_name": None, "category": None, "request_count": 2},
    ]
    assert [r.model_dump() for r in res.ngwaf_verified_bots_ts] == [
        {"time": "2026-10-18T03:00:00Z", "bot_name": "bingbot", "count": 4}
    ]


def test_filtered_ngwaf_sections_read_the_persisted_fact_keys():
    client = _Client()
    hs.security_aggregates(
        _service(client), _req(_COUNTRY_US), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
    )
    for tag in ("ngwaf_bots", "ngwaf_verified_bots_ts"):
        sql, params = client.call(tag)
        assert "FROM request_facts" in sql and "custom_fields['ngwaf_bot']" in sql
        assert "security_minute_dimensions" not in sql and params["filter_0_0"] == "US"


def test_ngwaf_not_configured_is_reported_from_service_config():
    with patch("backend.config.get_ngwaf_workspace_id", return_value=None):
        res = hs.security_aggregates(
            _service(_Client()), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    assert res.ngwaf_configured is False


def test_wellknown_bots_carry_real_verification_counters():
    client = _Client(
        {
            "wellknown_bots": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "66.249.66.1", "cnt": "8"},
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "203.0.113.9", "cnt": "2"},
            ]
        }
    )
    hostnames = {
        "66.249.66.1": ("crawl-66-249-66-1.googlebot.com", "resolved", True),
        "203.0.113.9": ("evil.example.net", "resolved", False),
    }
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="(?i)googlebot"),
        patch("backend.utils.bot_sources.get_hostnames", return_value=hostnames),
        patch("backend.utils.bot_sources.enqueue"),
    ):
        res = hs.security_aggregates(_service(client), _req(), START, END, sections={"wellknown_bots"})
    sql, params = client.call("wellknown_bots")
    assert "match(custom_fields['ua'], {bot_pattern:String})" in sql and params["bot_pattern"] == "(?i)googlebot"
    assert len(res.wellknown_bots) == 1
    bot = res.wellknown_bots[0]
    assert (bot.id, bot.request_count, bot.verified_count, bot.impersonator_count, bot.verification_coverage) == (
        "googlebot",
        10,
        8,
        2,
        1.0,
    )


def test_top_bots_classifies_user_agents_and_lists_ngwaf_bots():
    client = _Client(
        {
            "top_bots_ua": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"},
                {"ua": "curl/8", "cnt": "50"},
            ],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"}],
        }
    )
    with patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot):
        res = hs.top_bots(_service(client), _req(), START, END)
    assert [b.model_dump(exclude_unset=True) for b in res.bots] == [
        {"id": "googlebot", "name": "Googlebot", "category": "search-engine", "request_count": 30}
    ]
    assert [b.model_dump(exclude_unset=True) for b in res.ngwaf_bots] == [
        {"name": "bingbot", "category": "search", "request_count": 11}
    ]
    assert "LIMIT 10" in client.call("ngwaf_bots")[0]


def test_top_bots_honours_request_filters():
    client = _Client()
    hs.top_bots(_service(client), _req(_COUNTRY_US), START, END)
    assert all(params["filter_0_0"] == "US" for _, _, params in client.calls)


_CLIENT_ROW = {
    "ip": "198.51.100.77",
    "asn": "15169",
    "rtt_min_ms": 5.0,
    "tcp_rtt_ms": 100.0,
    "distance_km": 8800.4,
    "pop": "SJC",
    "client_lat": 51.5074,
    "client_lon": -0.1278,
    "pop_lat": 37.3382,
    "pop_lon": -121.8863,
    "impossible_distance": 1,
    "risk_level": "High",
    "country": "GB",
    "city": "",
}


def _proxy_client() -> _Client:
    return _Client(
        {
            "proxy_stats": [
                {"active_proxies_count": "3", "total_requests_count": "40", "distance_mismatches_count": "2"}
            ],
            "traffic_quality": [
                {"type": "Direct Connection", "count": "75"},
                {"type": "Active Tunnel / Proxy", "count": "25"},
            ],
            "suspicious_isps": [{"asn": "15169", "count": "25"}, {"asn": None, "count": "1"}],
            "active_clients": [_CLIENT_ROW],
        }
    )


def test_proxies_report_real_counts_quality_isps_and_clients():
    with patch("backend.core.duckdb.get_asn_names", return_value={15169: "Example Net"}):
        res = hs.security_aggregates(_service(_proxy_client()), _req(), START, END, sections={"proxies"})
    assert (res.active_proxies_count, res.tunnel_requests_count, res.distance_mismatches_count) == (3, 40, 2)
    assert [(q.label, q.value) for q in res.traffic_quality] == [
        ("Direct Connection", 75.0),
        ("Active Tunnel / Proxy", 25.0),
    ]
    assert [(i.isp, i.asn, i.count) for i in res.suspicious_isps] == [
        ("Example Net", 15169, 25),
        ("Unknown ISP", None, 1),
    ]
    c = res.active_clients[0]
    assert (c.ip, c.asn_name, c.impossible_distance, c.risk_level) == ("198.51.100.77", "Example Net", True, "High")
    assert (c.city, c.country, c.distance_km) == (None, "GB", 8800.4)


def test_proxies_mask_client_ips_for_analyst_sessions():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        masked = hs.security_aggregates(
            _service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=True
        )
        clear = hs.security_aggregates(
            _service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=False
        )
    assert masked.active_clients[0].ip == "198.51.100.xxx" and clear.active_clients[0].ip == "198.51.100.77"


def test_top_ips_header_masks_ips_for_analyst_sessions():
    client = _Client({"top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"top_ips_header"}, mask_ips=True)
    assert res.top_ips_header[0].ip == "10.0.1.xxx"


def test_proxies_section_is_served_inside_aggregates_with_masking():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.security_aggregates(_service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=True)
    assert res.active_proxies_count == 3 and res.active_clients[0].ip == "198.51.100.xxx"


def test_proxies_with_no_rows_report_zero_without_dividing_by_nothing():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.security_aggregates(_service(_Client()), _req(), START, END, sections={"proxies"})
    assert res.active_proxies_count == 0 and res.traffic_quality == [] and res.active_clients == []


def test_active_clients_list_high_risk_first():
    client = _proxy_client()
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        hs.security_aggregates(_service(client), _req(), START, END, sections={"proxies"})
    assert "ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2)" in client.call("active_clients")[0]


def test_clickhouse_failures_propagate_instead_of_rendering_empty_cards():
    client = _Client(error=RuntimeError("clickhouse unavailable"))
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.top_bots(_service(client), _req(), START, END)


def test_dashboard_bundle_serves_real_top_bots():
    client = _Client(
        {
            "top_bots_ua": [{"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"}],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "", "bot_cat": "search", "request_count": "11"}],
        }
    )
    request = SimpleNamespace(filters={}, bucket_seconds=3600, sections=None)
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch.object(hs_dashboard, "aggregates", return_value="aggregates"),
    ):
        out = hs_dashboard.bundle(_service(client), request, START, END)
    assert out["aggregates"] == "aggregates"
    assert [b.name for b in out["top_bots"].bots] == ["Googlebot"]
    assert [b.name for b in out["top_bots"].ngwaf_bots] == ["bingbot"]


# ── Merged from test_security_ngwaf.py ───────────────────────────────

NOW_security = datetime(2026, 9, 11, 20, 0, tzinfo=UTC)
_PG_TARGET = "backend.core.metadata.base.get_pg_readonly_connection"
_BOT_ROW = {"waf_req_id": "req-1", "bot_name": "bingbot", "category": "search", "wellknown_bot_name": "bing"}


class _Pg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, sql: str, params: list[str]) -> _Pg:
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        return None


class _AggClickHouse:
    def __init__(self, *, fail_domains: tuple[str, ...] = ()) -> None:
        self.batches: list[HighScaleBatch] = []
        self._fail_domains = fail_domains

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain in self._fail_domains:
            raise RuntimeError(f"insert failed for {batch.domain}")
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def rows(self, domain: str) -> list[dict[str, Any]]:
        return [row for b in self.batches if b.domain == domain for row in b.rows]

    def by_domain(self, domain: str) -> list[HighScaleBatch]:
        return [b for b in self.batches if b.domain == domain]


class _Catalog:
    def __init__(self, *manifests: Any) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id: str, domain: str, start: datetime, end: datetime) -> tuple[Any, ...]:
        return self._manifests


def _event(waf_req_id: str | None) -> dict[str, Any]:
    return {"timestamp": "2026-09-11T20:00:00Z", "url": "/ok", **({"waf_req_id": waf_req_id} if waf_req_id else {})}


def test_enrichment_tags_events_whose_waf_req_id_is_in_the_bot_cache():
    events = (_event("req-1"), _event("req-unknown"), _event(None))
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _enrich_high_scale_ngwaf_bots(events)
    tagged, unknown, bare = events
    assert tagged["_ngwaf_bot_name"] == "bingbot" and tagged["ngwaf_bot_verification_state"] == "verified"
    assert persisted_fact_fields(tagged) == {
        "ngwaf_bot": "bingbot",
        "ngwaf_bot_category": "search",
        "ngwaf_wellknown_bot": "bing",
    }
    assert not {k for u in (unknown, bare) for k in u if "ngwaf" in k}


def test_enrichment_never_invents_bots_for_synthetic_looking_ids():
    events = (_event("waf-123"),)
    with patch(_PG_TARGET, return_value=_Pg([])):
        _enrich_high_scale_ngwaf_bots(events)
    assert not {k for k in events[0] if "ngwaf" in k}


def test_enrichment_failure_is_logged_not_silent(caplog: pytest.LogCaptureFixture):
    events = (_event("req-1"),)
    with patch(_PG_TARGET, side_effect=RuntimeError("pg down")), caplog.at_level("WARNING"):
        _enrich_high_scale_ngwaf_bots(events)
    assert "NGWAF bot enrichment failed" in caplog.text and "_ngwaf_bot_name" not in events[0]


def _controller() -> tuple[HighScaleIngestController, InMemoryObjectStore, _AggClickHouse, _AggClickHouse]:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    objects, facts, aggregates = InMemoryObjectStore(), _AggClickHouse(), _AggClickHouse()
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), facts),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregates),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    return controller, objects, facts, aggregates


def _ingest(controller: HighScaleIngestController) -> Any:
    payload = gzip.compress((str(_event("req-1")).replace("'", '"') + "\n").encode())
    desc = SourceObjectDescriptor("raw/request/one.gz", "sha256:source", len(payload), "v1")
    return controller.ingest_batch(
        service_id="svc", domain="request", sources_payloads=[(desc, payload)], now=NOW_security
    )


def test_ingest_persists_the_bot_on_the_fact_row_and_projects_it():
    controller, _, facts, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _ingest(controller)
    assert persisted_fact_fields(facts.rows("request")[0])["ngwaf_bot"] == "bingbot"
    projected = aggregates.rows("security_dimensions")
    assert [(r["dimension"], r["value"], r["requests"], r["verified_count"]) for r in projected] == [
        ("ngwaf_bot", "bingbot", 1, 1)
    ]
    assert projected[0]["wellknown_bot_name"] == "bing" and projected[0]["bot_category"] == "search"


def test_archive_stays_raw_so_a_rebuild_can_enrich_against_the_current_cache():
    controller, objects, _, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    assert aggregates.rows("security_dimensions") == []
    rebuilt = _AggClickHouse()
    pub = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        replayed = replay_security_projections(result.manifest, ArchivePublication(objects), pub, rebuild_id="drill-1")
    assert (replayed.rows_read, replayed.rows_published) == (1, 1)
    assert [b.domain for b in rebuilt.batches] == ["security_dimensions"]
    assert rebuilt.batches[0].batch_id.startswith("rebuild-security:drill-1:")
    assert rebuilt.rows("security_dimensions")[0]["value"] == "bingbot"


def test_replay_publishes_nothing_when_no_event_resolves_to_a_bot():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
        replayed = replay_security_projections(
            result.manifest,
            ArchivePublication(objects),
            ClickHousePublication(InMemoryBatchManifestStore(), _AggClickHouse()),
            rebuild_id="drill-2",
        )
    assert (replayed.rows_read, replayed.rows_published) == (1, 0)


def test_rebuild_from_fos_replays_every_request_manifest_and_resumes_idempotently():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    archive, rebuilt = ArchivePublication(objects), _AggClickHouse()
    pub = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)
    now_plus = NOW_security + timedelta(hours=1)
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        first = rebuild_security_projections_from_fos(
            _Catalog(result.manifest), archive, pub, service_id="svc", now=now_plus
        )
        second = rebuild_security_projections_from_fos(
            _Catalog(result.manifest), archive, pub, service_id="svc", now=now_plus, rebuild_id=first.rebuild_id
        )
    assert first.full_rebuild_complete is True
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1 and len(rebuilt.batches) == 1


def test_rebuild_reports_a_failed_manifest_instead_of_hiding_it():
    controller, _, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    report = rebuild_security_projections_from_fos(
        _Catalog(result.manifest),
        ArchivePublication(InMemoryObjectStore()),
        ClickHousePublication(InMemoryBatchManifestStore(), _AggClickHouse()),
        service_id="svc",
        now=NOW_security + timedelta(hours=1),
    )
    assert report.full_rebuild_complete is False and report.manifests_replayed == 0
    assert "failed to rebuild Security projections" in report.errors[0]


def test_rebuild_rejects_a_non_positive_window():
    with pytest.raises(ValueError, match="window must be positive"):
        rebuild_security_projections_from_fos(
            _Catalog(),
            ArchivePublication(InMemoryObjectStore()),
            ClickHousePublication(InMemoryBatchManifestStore(), _AggClickHouse()),
            service_id="svc",
            window=timedelta(0),
        )


# ── Merged from tests/high_scale/test_cmcd.py ──

START_cmcd = "2026-09-14T10:00:00Z"
END_cmcd = "2026-09-14T10:15:00Z"
ALL_SECTIONS = {
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "startup_ts",
    "session_duration_dist",
}
_PRESENT = {
    "events": 40,
    "sessions": 4,
    **dict.fromkeys(
        (
            "has_bl",
            "has_br",
            "has_bs",
            "has_mtp",
            "has_ot",
            "has_sf",
            "has_su",
            "has_tb",
            "has_cid",
            "has_country",
            "has_asn",
        ),
        1,
    ),
}
_CMCD_CANNED: dict[str, list[dict[str, Any]]] = {
    "cmcd:overview_peak": [{"peak_viewers": 4}],
    "cmcd:overview_duration": [{"avg_duration": 125.4}],
    "cmcd:overview": [
        {
            "active_sessions": 4,
            "rebuffer_rate": 25.0,
            "avg_bitrate": 2500.126,
            "avg_buffer_length": 12.0,
            "median_throughput": 8000.0,
        }
    ],
    "cmcd:timeseries": [
        {
            "bucket": "2026-09-14 10:05:00",
            "concurrent_sessions": 3,
            "rebuffer_session_pct": 33.333,
            "p50_buffer": 10.0,
            "p95_buffer": 20.0,
            "starvation_rate": 5.0,
            "avg_bitrate": 2400.0,
            "utilization_ratio": 0.51234,
            "p50_throughput": 7000.0,
            "p95_throughput": 9000.0,
            "p99_throughput": 9500.0,
            "startup_ratio": 2.5,
        }
    ],
    "cmcd:new_sessions": [{"bucket": "2026-09-14 10:05:00", "new_sessions": 2}],
    "cmcd:top_content": [
        {"content_id": "movie-1", "session_count": 3, "rebuffer_rate": 33.33, "avg_br": 3000.0, "avg_bl": 9.5}
    ],
    "cmcd:rebuffer_by_country": [{"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_mtp": 7500.0}],
    "cmcd:rebuffer_by_asn": [{"asn": 7922, "rebuffer_rate": 50.0, "session_count": 2, "avg_br": 2000.0, "avg_bl": 8.0}],
    "cmcd:object_type_dist": [{"object_type": "v", "request_count": 30}, {"object_type": "a", "request_count": 10}],
    "cmcd:streaming_format_dist": [{"streaming_format": "h", "session_count": 4}],
    "cmcd:session_duration_dist": [{"sort_order": 2, "session_count": 1}, {"sort_order": 5, "session_count": 3}],
}


class _Client_cmcd:
    def __init__(self, probe: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.scopes: list[dict] = []
        self._probe = probe if probe is not None else _PRESENT

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        self.scopes.append(current_clickhouse_scope())
        if "cmcd:probe" in sql:
            return [self._probe]
        for marker, rows in _CMCD_CANNED.items():
            if marker in sql:
                return rows
        raise AssertionError(sql)


def _request(**overrides: Any) -> CmcdRequest:
    return CmcdRequest(**{"bucket_seconds": 300, "top_n": 10, **overrides})


def _run(client: _Client_cmcd, req: CmcdRequest | None = None, sections: set[str] | None = None, **kwargs: Any):
    service = SimpleNamespace(service_id="svc", client=client)
    return cmcd_aggregates(
        service, req or _request(), START_cmcd, END_cmcd, ALL_SECTIONS if sections is None else sections, False
    )


@pytest.fixture()
def _asn_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda service_id, asns: {7922: "Comcast Cable Communications"})


@pytest.mark.usefixtures("_asn_names")
def test_probe_and_multicall_overview_share_explicit_projected_fact_ownership() -> None:
    client = _Client_cmcd()
    result = _run(client, sections={"overview"})
    assert len(client.calls) == 4
    assert [row["section"] for row in client.scopes] == ["cmcd.probe"] + ["cmcd.overview"] * 3
    assert all(row["source"] == "projected_fact" and row["service_id"] == "svc" for row in client.scopes)
    assert len({row["section_id"] for row in client.scopes[1:]}) == 1
    assert len(result["section_timings"]) == 2 and result["section_timings"][0]["section"] == "cmcd.probe"
    hit = _run(client, sections={"overview"})
    assert len(client.calls) == 4 and hit["_response_cache"]["state"] == "hit" and "section_timings" not in hit


@pytest.mark.usefixtures("_asn_names")
def test_no_cmcd_events_in_the_window_reports_no_data_after_a_single_probe() -> None:
    client = _Client_cmcd(probe={**_PRESENT, "events": 0, "sessions": 0})
    result = _run(client)
    timings = result.pop("section_timings")
    assert len(timings) == 1
    assert (timings[0]["section"], timings[0]["source"], timings[0]["outcome"]) == (
        "cmcd.probe",
        "projected_fact",
        "success",
    )
    assert result == {
        "available": True,
        "has_data": False,
        "is_cached": False,
        "_response_cache": {"state": "miss", "ttl_seconds": 30.0, "prewarmer": False, "single_flight": "leader"},
    }
    hit = _run(client)
    assert hit["_response_cache"]["state"] == "hit" and hit["is_cached"] is True and len(client.calls) == 1


@pytest.mark.usefixtures("_asn_names")
def test_every_section_is_computed_from_the_projection() -> None:
    result = _run(_Client_cmcd())
    assert result["available"] is True and result["has_data"] is True
    assert result["overview"] == {
        "active_sessions": 4,
        "rebuffer_rate": 25.0,
        "avg_bitrate": 2500.13,
        "avg_buffer_length": 12.0,
        "median_throughput": 8000.0,
        "peak_viewers": 4,
        "avg_session_duration": 125,
    }
    assert result["top_content"] == [
        {
            "content_id": "movie-1",
            "session_count": 3,
            "rebuffer_rate": 33.33,
            "avg_bitrate": 3000.0,
            "avg_buffer_length": 9.5,
        }
    ]
    assert result["rebuffer_by_country"] == [
        {"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_throughput": 7500.0}
    ]
    assert result["rebuffer_by_asn"] == [
        {
            "asn": 7922,
            "label": "Comcast Cable Communications (7922)",
            "rebuffer_rate": 50.0,
            "session_count": 2,
            "avg_bitrate": 2000.0,
            "avg_buffer_length": 8.0,
        }
    ]
    assert result["object_type_dist"] == [
        {"object_type": "v", "request_count": 30},
        {"object_type": "a", "request_count": 10},
    ]
    assert result["streaming_format_dist"] == [{"streaming_format": "h", "session_count": 4}]
    assert result["session_duration_dist"] == [
        {"duration_bucket": "30s–2m", "session_count": 1},
        {"duration_bucket": "10–30m", "session_count": 3},
    ]


@pytest.mark.usefixtures("_asn_names")
def test_time_series_are_padded_to_every_bucket_in_the_window() -> None:
    result = _run(_Client_cmcd())
    buckets = [point["bucket"] for point in result["sessions_ts"]]
    assert buckets == [
        "2026-09-14 10:00:00",
        "2026-09-14 10:05:00",
        "2026-09-14 10:10:00",
        "2026-09-14 10:15:00",
    ]
    assert result["sessions_ts"][1] == {
        "bucket": "2026-09-14 10:05:00",
        "concurrent_sessions": 3,
        "rebuffer_session_pct": 33.33,
        "new_sessions": 2,
    }
    assert result["sessions_ts"][0] == {
        "bucket": "2026-09-14 10:00:00",
        "concurrent_sessions": 0,
        "rebuffer_session_pct": None,
        "new_sessions": 0,
    }
    assert result["buffer_health_ts"][1] == {
        "bucket": "2026-09-14 10:05:00",
        "p50_buffer": 10.0,
        "p95_buffer": 20.0,
        "starvation_rate": 5.0,
    }
    assert result["bitrate_ts"][1]["utilization_ratio"] == 0.51
    assert result["throughput_ts"][1] == {"bucket": "2026-09-14 10:05:00", "p50": 7000.0, "p95": 9000.0, "p99": 9500.0}
    assert result["startup_ts"][1] == {"bucket": "2026-09-14 10:05:00", "startup_ratio": 2.5}
    assert result["startup_ts"][3] == {"bucket": "2026-09-14 10:15:00", "startup_ratio": None}


@pytest.mark.usefixtures("_asn_names")
def test_the_four_time_series_share_one_scan() -> None:
    client = _Client_cmcd()
    _run(client, sections={"sessions_ts", "buffer_health_ts", "bitrate_ts", "throughput_ts", "startup_ts"})
    assert len([sql for sql, _ in client.calls if "cmcd:timeseries" in sql]) == 1


@pytest.mark.usefixtures("_asn_names")
def test_only_requested_sections_are_queried() -> None:
    client = _Client_cmcd()
    result = _run(client, sections={"object_type_dist"})
    assert [sql.split("*/")[0].split("/*")[1].strip() for sql, _ in client.calls] == [
        "cmcd:probe",
        "cmcd:object_type_dist",
    ]
    assert "overview" not in result and "top_content" not in result


@pytest.mark.usefixtures("_asn_names")
def test_sections_that_need_an_absent_cmcd_key_are_omitted_like_the_standard_path() -> None:
    client = _Client_cmcd(probe={**_PRESENT, "has_bs": 0, "has_br": 0, "has_cid": 0, "has_country": 0, "has_asn": 0})
    result = _run(client)
    for key in ("top_content", "rebuffer_by_country", "rebuffer_by_asn", "bitrate_ts"):
        assert key not in result
    assert result["overview"]["rebuffer_rate"] is None and result["overview"]["avg_bitrate"] is None
    assert result["overview"]["avg_buffer_length"] == 12.0
    markers = " ".join(sql for sql, _ in client.calls)
    assert "cmcd:top_content" not in markers and "cmcd:rebuffer_by_country" not in markers


@pytest.mark.usefixtures("_asn_names")
def test_all_queries_are_scoped_to_the_service_published_batches_and_window() -> None:
    client = _Client_cmcd()
    _run(client)
    for sql, params in client.calls:
        assert "FROM cmcd_projection_facts" in sql and "service_id={service_id:String}" in sql
        assert "publication_state='visible'" in sql
        assert "high_scale_batch_publications FINAL" in sql and "domain='cmcd'" in sql
        assert params["service_id"] == "svc"
        assert "event_timestamp >= {start:DateTime64(3)}" in sql and "event_timestamp <= {end:DateTime64(3)}" in sql
    assert any(params.get("bucket_seconds") == 300 for _, params in client.calls)
    assert any(params.get("top_n") == 10 for _, params in client.calls)


@pytest.mark.usefixtures("_asn_names")
def test_country_and_asn_filters_are_applied_not_dropped() -> None:
    client = _Client_cmcd()
    req = _request(
        filters={
            "country": {"mode": "include", "values": ["US", "CA"]},
            "asn": {"mode": "exclude", "values": ["7922"]},
        }
    )
    _run(client, req, sections={"overview"})
    for sql, params in client.calls:
        assert "country IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "asn NOT IN ({flt_1_0:UInt32})" in sql
        assert params["flt_0_0"] == "US" and params["flt_0_1"] == "CA" and params["flt_1_0"] == 7922


@pytest.mark.usefixtures("_asn_names")
def test_filters_the_projection_cannot_honour_raise_instead_of_being_ignored() -> None:
    with pytest.raises(ValueError, match="url"):
        _run(_Client_cmcd(), _request(filters={"url": {"mode": "include", "values": ["/a"]}}))


@pytest.mark.usefixtures("_asn_names")
def test_non_numeric_asn_filter_values_are_rejected() -> None:
    with pytest.raises(ValueError, match="asn"):
        _run(_Client_cmcd(), _request(filters={"asn": {"mode": "include", "values": ["AS7922"]}}))


@pytest.mark.usefixtures("_asn_names")
def test_output_validates_against_the_standard_response_model() -> None:
    model = CmcdAggregatesResponse.model_validate(_run(_Client_cmcd()))
    assert model.overview is not None and model.overview.peak_viewers == 4
    assert model.rebuffer_by_asn[0].label.endswith("(7922)")


@pytest.mark.usefixtures("_asn_names")
def test_probe_failures_surface_instead_of_becoming_empty_charts() -> None:
    class _Down(_Client_cmcd):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            raise RuntimeError("clickhouse unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        _run(_Down())


@pytest.mark.usefixtures("_asn_names")
def test_bucket_labels_use_the_clickhouse_minute_specifier() -> None:
    client = _Client_cmcd()
    _run(client, sections={"sessions_ts"})
    timeseries = next(sql for sql, _ in client.calls if "cmcd:timeseries" in sql)
    assert "'%Y-%m-%d %H:%i:%S'" in timeseries and "%M" not in timeseries


# ── Merged from test_cmcd_projection_wiring.py ───────────────────────

_ClickHouse_cmcd = _AggClickHouse


def _controller_cmcd(clickhouse: _AggClickHouse, *, control=None) -> HighScaleIngestController:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    return HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        control_plane=control,
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )


def _payload(*lines: dict) -> bytes:
    return gzip.compress("\n".join(json.dumps(line) for line in lines).encode())


_CMCD_LINE = {"timestamp": "2026-09-11T20:00:00Z", "url": "/seg/1.m4s", "cmcd_br": "1200", "cmcd_sid": "sess-1"}
_PLAIN_LINE = {"timestamp": "2026-09-11T20:00:01Z", "url": "/index.html"}


def _ingest_cmcd(controller: HighScaleIngestController, *lines: dict, domain: str = "request"):
    payload = _payload(*lines)
    desc = SourceObjectDescriptor("raw/request/one.gz", "sha256:one", len(payload), "v1")
    return controller.ingest_batch(
        service_id="svc",
        domain=domain,
        sources_payloads=[(desc, payload)],
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )


@pytest.mark.usefixtures("_asn_names")
def test_request_ingest_publishes_cmcd_projection_rows_for_cmcd_events_only() -> None:
    clickhouse = _ClickHouse_cmcd()
    result = _ingest_cmcd(_controller_cmcd(clickhouse), _CMCD_LINE, _PLAIN_LINE)
    assert result.publication.rows_visible == 2
    (request_batch,) = clickhouse.by_domain("request")
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    cmcd_event = next(row for row in request_batch.rows if row["cmcd"])
    (projection,) = cmcd_batch.rows
    assert projection["request_event_id"] == cmcd_event["event_id"]
    assert projection["projection_key"] == build_cmcd_projection_key({"request_event_id": cmcd_event["event_id"]})
    assert projection["fields"]["br"] == "1200" and projection["service_id"] == "svc"
    assert cmcd_batch.generation == request_batch.generation


@pytest.mark.usefixtures("_asn_names")
def test_projection_rows_are_accepted_by_the_cmcd_fact_table_mapping() -> None:
    clickhouse = _ClickHouse_cmcd()
    _ingest_cmcd(_controller_cmcd(clickhouse), _CMCD_LINE)
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    (row,) = _rows_for_domain(cmcd_batch, "00000000-0000-0000-0000-000000000001")
    assert row[0] == "svc"


@pytest.mark.usefixtures("_asn_names")
def test_requests_without_cmcd_fields_publish_no_projection_batch() -> None:
    clickhouse = _ClickHouse_cmcd()
    _ingest_cmcd(_controller_cmcd(clickhouse), _PLAIN_LINE)
    assert clickhouse.by_domain("cmcd") == [] and len(clickhouse.by_domain("request")) == 1


@pytest.mark.usefixtures("_asn_names")
def test_non_request_domains_do_not_project_cmcd() -> None:
    clickhouse = _ClickHouse_cmcd()
    line = {"timestamp": "2026-09-11T20:00:00Z", "metric_name": "LCP", "metric_value": 1.5, "cmcd_br": "1"}
    _ingest_cmcd(_controller_cmcd(clickhouse), line, domain="rum_vitals")
    assert clickhouse.by_domain("cmcd") == []


@pytest.mark.usefixtures("_asn_names")
def test_projection_failure_never_fails_the_request_ingest(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse_cmcd(fail_domains=("cmcd",))
    with caplog.at_level(logging.ERROR, logger="backend.high_scale.ingest_controller"):
        result = _ingest_cmcd(_controller_cmcd(clickhouse), _CMCD_LINE)
    assert result.publication.rows_visible == 1 and len(clickhouse.by_domain("request")) == 1
    assert any("cmcd projection publish failed" in r.getMessage() for r in caplog.records)


@pytest.mark.usefixtures("_asn_names")
def test_republishing_the_same_batch_id_is_idempotent() -> None:
    clickhouse = _ClickHouse_cmcd()
    controller = _controller_cmcd(clickhouse)
    events = [{"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}]
    controller._publish_cmcd_projection("svc", "batch-1", 1, events)
    controller._publish_cmcd_projection("svc", "batch-1", 1, events)
    assert len(clickhouse.by_domain("cmcd")) == 1


@pytest.mark.usefixtures("_asn_names")
def test_malformed_events_are_skipped_without_losing_valid_ones(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse_cmcd()
    controller = _controller_cmcd(clickhouse)
    good = {"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}
    not_a_mapping = {"event_id": "evt-2", "service_id": "svc", "timestamp": "2026-09-11T20:00:01Z", "cmcd": "br=1"}
    no_event_id = {"service_id": "svc", "timestamp": "2026-09-11T20:00:02Z", "cmcd": {"br": "1"}}
    with caplog.at_level(logging.WARNING, logger="backend.high_scale.ingest_controller"):
        controller._publish_cmcd_projection("svc", "batch-2", 1, [good, not_a_mapping, no_event_id])
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert [row["request_event_id"] for row in cmcd_batch.rows] == ["evt-1"]
    assert any(getattr(r, "skipped", None) == 2 for r in caplog.records)


@pytest.mark.usefixtures("_asn_names")
def test_batch_ingest_publishes_a_manifest_scoped_cmcd_batch() -> None:
    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    control.register_archive_manifest.return_value = MagicMock(state=ArchiveState.ARTIFACT_UPLOADING)
    control.archive_manifest.side_effect = KeyError("not found yet")
    clickhouse = _ClickHouse_cmcd()
    controller = _controller_cmcd(clickhouse, control=control)
    sources = []
    for index, line in enumerate((_CMCD_LINE, _PLAIN_LINE, {**_CMCD_LINE, "url": "/seg/2.m4s"})):
        payload = _payload(line)
        descriptor = MagicMock(object_key=f"raw/request/part_{index}.gz", checksum=f"sha256:{index}", version="v1")
        descriptor.size_bytes = len(payload)
        sources.append((descriptor, payload))
    result = controller.ingest_batch(service_id="svc", domain="request", sources_payloads=sources, next_cursor="c-3")
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert cmcd_batch.batch_id == build_batch_id_from_manifest_id(f"{result.manifest.manifest_id}:cmcd")
    assert len(cmcd_batch.rows) == 2 and len(clickhouse.by_domain("request")[0].rows) == 3


@pytest.mark.usefixtures("_asn_names")
def test_probe_and_asn_enrichment_are_disjoint_overhead(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [0.0]
    lookup_scopes: list[dict[str, Any]] = []
    monkeypatch.setattr("backend.core.request_telemetry.time.perf_counter", lambda: clock[0])

    class TimedClient(_Client_cmcd):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            clock[0] += 0.01 if "cmcd:probe" in sql else 0.02
            return super().execute(sql, params)

    def names(service_id: str, asns: list[int]) -> dict[int, str]:
        assert service_id == "svc" and asns == [7922]
        lookup_scopes.append(current_clickhouse_scope())
        clock[0] += 0.03
        return {7922: "Example Network"}

    monkeypatch.setattr(hs_cmcd, "_asn_names", names)
    client = TimedClient()
    result = _run(client, sections={"rebuffer_by_asn"})
    timings = {row["section"]: row for row in result["section_timings"]}
    assert set(timings) == {"cmcd.probe", "cmcd.rebuffer_asn", "cmcd.asn_labels"}
    assert timings["cmcd.probe"]["activity"] == "overhead" and timings["cmcd.probe"]["time_ms"] == pytest.approx(10)
    assert timings["cmcd.rebuffer_asn"]["activity"] == "compute"
    assert timings["cmcd.rebuffer_asn"]["time_ms"] == pytest.approx(20)
    assert timings["cmcd.asn_labels"]["activity"] == "overhead"
    assert timings["cmcd.asn_labels"]["time_ms"] == pytest.approx(30)
    assert timings["cmcd.asn_labels"]["source"] == "unknown"
    assert lookup_scopes[0]["section_id"] == timings["cmcd.asn_labels"]["section_id"]
    assert all(row["parent_section_id"] is None for row in timings.values())
    assert all(row["timing_kind"] == "client_section_wall" and row["server_cpu_ms"] is None for row in timings.values())
    assert all(row["source"] == "projected_fact" for row in client.scopes)
    assert {row["section_id"] for row in client.scopes} == {
        timings["cmcd.probe"]["section_id"],
        timings["cmcd.rebuffer_asn"]["section_id"],
    }
    assert result["rebuffer_by_asn"][0]["label"] == "Example Network (7922)"
    assert result["_response_cache"]["state"] == "miss" and result["_response_cache"]["single_flight"] == "leader"
    hit = _run(client, sections={"rebuffer_by_asn"})
    assert hit["_response_cache"]["state"] == "hit"
    assert hit["_response_cache"]["ttl_seconds"] == pytest.approx(hs_cmcd._memo.cache.ttl)
    assert hit["_response_cache"]["prewarmer"] is False and "section_timings" not in hit
    assert len(lookup_scopes) == 1 and len(client.calls) == 2
    assert current_clickhouse_scope()["section_id"] is None


@pytest.mark.usefixtures("_asn_names")
def test_asn_enrichment_failure_is_overhead_and_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    observations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "backend.core.request_telemetry._logger.info", lambda event, **entry: observations.append(entry)
    )

    def fails(service_id: str, asns: list[int]) -> dict[int, str]:
        raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(hs_cmcd, "_asn_names", fails)
    client = _Client_cmcd()
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        _run(client, sections={"rebuffer_by_asn"})
    by_section = {row["section"]: row for row in observations}
    assert by_section["cmcd.rebuffer_asn"]["outcome"] == "success"
    assert (
        by_section["cmcd.asn_labels"]["outcome"] == "error" and by_section["cmcd.asn_labels"]["activity"] == "overhead"
    )
    assert current_clickhouse_scope()["section_id"] is None and len(hs_cmcd._memo.cache) == 0


@pytest.mark.usefixtures("_asn_names")
def test_all_cmcd_tasks_preserve_request_and_page_correlation() -> None:
    client = _Client_cmcd()
    request_token = current_attribution.set(Attribution(kind="admin", request_id="request-cmcd"))
    page_token = telemetry._PAGE_LOAD_ID.set("page-cmcd")
    try:
        result = _run(client)
    finally:
        telemetry._PAGE_LOAD_ID.reset(page_token)
        current_attribution.reset(request_token)
    timings = {row["section_id"]: row for row in result["section_timings"]}
    expected_sections = {
        "cmcd.probe",
        "cmcd.overview",
        "cmcd.timeseries",
        "cmcd.top_content",
        "cmcd.rebuffer_country",
        "cmcd.rebuffer_asn",
        "cmcd.object_type",
        "cmcd.streaming_format",
        "cmcd.session_duration",
        "cmcd.asn_labels",
    }
    assert {row["section"] for row in timings.values()} == expected_sections
    for scope in client.scopes:
        section = timings[scope["section_id"]]
        assert section["section"] == scope["section"]
        assert section["request_id"] == scope["request_id"] == "request-cmcd"
        assert section["page_load_id"] == scope["page_load_id"] == "page-cmcd"
        assert section["service_id"] == scope["service_id"] == "svc"
        assert section["source"] == scope["source"] == "projected_fact"
        assert section["server_cpu_ms"] is None
    ts_section = timings[next(row["section_id"] for row in client.scopes if row["section"] == "cmcd.timeseries")]
    assert ts_section["executor_dispatch"] == "thread_pool" and current_clickhouse_scope()["section_id"] is None


_security_cases = SimpleNamespace(START=START, END=END, _Client=_Client, _service=_service, _req=_req)
_cmcd_cases = SimpleNamespace(START=START_cmcd, END=END_cmcd, _Client=_Client_cmcd, _request=_request, _run=_run)
_sys.modules.setdefault("tests.high_scale.test_security", _security_cases)  # type: ignore[arg-type]
_sys.modules.setdefault("tests.high_scale.test_cmcd", _cmcd_cases)  # type: ignore[arg-type]
