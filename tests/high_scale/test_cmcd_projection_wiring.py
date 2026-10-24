"""CMCD projection wiring: request ingest publishes project_cmcd rows."""

from __future__ import annotations

import gzip
import json
import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    _rows_for_domain,
)
from backend.high_scale.schema import build_batch_id_from_manifest_id, build_cmcd_projection_key
from backend.high_scale.source_discovery import SourceObjectDescriptor
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


class _ClickHouse:
    def __init__(self, *, fail_domains: tuple[str, ...] = ()) -> None:
        self.batches: list[HighScaleBatch] = []
        self._fail_domains = fail_domains

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain in self._fail_domains:
            raise RuntimeError(f"insert failed for {batch.domain}")
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def by_domain(self, domain: str) -> list[HighScaleBatch]:
        return [b for b in self.batches if b.domain == domain]


def _controller(clickhouse: _ClickHouse, *, control=None) -> HighScaleIngestController:
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


def _ingest(controller: HighScaleIngestController, *lines: dict, domain: str = "request"):
    payload = _payload(*lines)
    desc = SourceObjectDescriptor("raw/request/one.gz", "sha256:one", len(payload), "v1")
    return controller.ingest_batch(
        service_id="svc",
        domain=domain,
        sources_payloads=[(desc, payload)],
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )


def test_request_ingest_publishes_cmcd_projection_rows_for_cmcd_events_only() -> None:
    clickhouse = _ClickHouse()

    result = _ingest(_controller(clickhouse), _CMCD_LINE, _PLAIN_LINE)

    assert result.publication.rows_visible == 2
    (request_batch,) = clickhouse.by_domain("request")
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    cmcd_event = next(row for row in request_batch.rows if row["cmcd"])
    (projection,) = cmcd_batch.rows
    assert projection["request_event_id"] == cmcd_event["event_id"]
    assert projection["projection_key"] == build_cmcd_projection_key({"request_event_id": cmcd_event["event_id"]})
    assert projection["fields"]["br"] == "1200"
    assert projection["service_id"] == "svc"
    assert cmcd_batch.generation == request_batch.generation


def test_projection_rows_are_accepted_by_the_cmcd_fact_table_mapping() -> None:
    clickhouse = _ClickHouse()
    _ingest(_controller(clickhouse), _CMCD_LINE)

    (cmcd_batch,) = clickhouse.by_domain("cmcd")

    (row,) = _rows_for_domain(cmcd_batch, "00000000-0000-0000-0000-000000000001")
    assert row[0] == "svc"


def test_requests_without_cmcd_fields_publish_no_projection_batch() -> None:
    clickhouse = _ClickHouse()

    _ingest(_controller(clickhouse), _PLAIN_LINE)

    assert clickhouse.by_domain("cmcd") == []
    assert len(clickhouse.by_domain("request")) == 1


def test_non_request_domains_do_not_project_cmcd() -> None:
    clickhouse = _ClickHouse()
    line = {"timestamp": "2026-09-11T20:00:00Z", "metric_name": "LCP", "metric_value": 1.5, "cmcd_br": "1"}

    _ingest(_controller(clickhouse), line, domain="rum_vitals")

    assert clickhouse.by_domain("cmcd") == []


def test_projection_failure_never_fails_the_request_ingest(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse(fail_domains=("cmcd",))

    with caplog.at_level(logging.ERROR, logger="backend.high_scale.ingest_controller"):
        result = _ingest(_controller(clickhouse), _CMCD_LINE)

    assert result.publication.rows_visible == 1
    assert len(clickhouse.by_domain("request")) == 1
    assert any("cmcd projection publish failed" in r.getMessage() for r in caplog.records)


def test_republishing_the_same_batch_id_is_idempotent() -> None:
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse)
    events = [{"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}]

    controller._publish_cmcd_projection("svc", "batch-1", 1, events)
    controller._publish_cmcd_projection("svc", "batch-1", 1, events)

    assert len(clickhouse.by_domain("cmcd")) == 1


def test_malformed_events_are_skipped_without_losing_valid_ones(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse)
    good = {"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}
    not_a_mapping = {"event_id": "evt-2", "service_id": "svc", "timestamp": "2026-09-11T20:00:01Z", "cmcd": "br=1"}
    no_event_id = {"service_id": "svc", "timestamp": "2026-09-11T20:00:02Z", "cmcd": {"br": "1"}}

    with caplog.at_level(logging.WARNING, logger="backend.high_scale.ingest_controller"):
        controller._publish_cmcd_projection("svc", "batch-2", 1, [good, not_a_mapping, no_event_id])

    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert [row["request_event_id"] for row in cmcd_batch.rows] == ["evt-1"]
    assert any(getattr(r, "skipped", None) == 2 for r in caplog.records)


def test_batch_ingest_publishes_a_manifest_scoped_cmcd_batch() -> None:
    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    control.register_archive_manifest.return_value = MagicMock(state=ArchiveState.ARTIFACT_UPLOADING)
    control.archive_manifest.side_effect = KeyError("not found yet")
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse, control=control)
    sources = []
    for index, line in enumerate((_CMCD_LINE, _PLAIN_LINE, {**_CMCD_LINE, "url": "/seg/2.m4s"})):
        payload = _payload(line)
        descriptor = MagicMock(object_key=f"raw/request/part_{index}.gz", checksum=f"sha256:{index}", version="v1")
        descriptor.size_bytes = len(payload)
        sources.append((descriptor, payload))

    result = controller.ingest_batch(service_id="svc", domain="request", sources_payloads=sources, next_cursor="c-3")

    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert cmcd_batch.batch_id == build_batch_id_from_manifest_id(f"{result.manifest.manifest_id}:cmcd")
    assert len(cmcd_batch.rows) == 2
    assert len(clickhouse.by_domain("request")[0].rows) == 3
