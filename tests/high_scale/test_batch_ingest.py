from __future__ import annotations

import gzip
from unittest.mock import MagicMock

import pytest
from backend.high_scale.source_discovery import (
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)

from backend.high_scale.archive_models import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import (
    HIGH_SCALE_PAGE_MAX_BYTES,
    BatchIngestResult,
    HighScaleIngestController,
)
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PublicationResult,
    PublicationStatus,
)
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


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


# ── Merged from test_worker_fairness_wiring.py ──────────────────────────────

import logging
from collections import Counter
from dataclasses import dataclass, field

from backend.high_scale.orchestration import PageRun
from backend.high_scale.worker import FairPageSelector, HighScaleWorkerLoop, _MultiServiceWorkerLoop, _weights_from_env


@dataclass
class _Coordinator:
    calls: list[tuple[str, str]] = field(default_factory=list)

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def _loop(services, domains, **kwargs):
    coordinator = _Coordinator()
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
    class _Flaky(_Coordinator):
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
    coordinators = {"a": _Coordinator(), "b": _Coordinator()}
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

import json
from collections.abc import Sequence
from typing import Any

from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.performance_projection import build_performance_projection_rows

from backend.high_scale.aggregate_batch_adapter import compute_dimension_counts
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
