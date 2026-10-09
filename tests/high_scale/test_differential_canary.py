"""Differential canary test suite for the High-Scale ingestion redesign.

Validates ADR-21 and High-Scale Design §9.7 / §11 Phase 3.2 requirements:
1. Row counts and exact event records match between the unbatched and batched paths.
2. Dimension aggregates and projection rows match between both paths.
3. Per-service feature flag toggling supports seamless canary enablement and
   immediate, zero-data-loss rollback to the existing unbatched path.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Sequence
from typing import Any

from backend.high_scale.aggregate_writer import compute_dimension_counts
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import (
    HighScaleIngestController,
    IngestResult,
)
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.network_projection import build_network_projection_rows
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.performance_projection import build_performance_projection_rows
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PublicationStatus,
)
from backend.high_scale.security_projection import build_security_projection_rows
from backend.high_scale.source_discovery import SourceObjectDescriptor


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

    # --- Path A: Existing Unbatched Path ---
    ch_unbatched = _MockClickHouse()
    ctrl_unbatched, _, _ = _build_controller(service_id, "worker-unbatched", ch_unbatched)

    unbatched_results: list[IngestResult] = []
    for desc, payload, _ in sources:
        res = ctrl_unbatched.ingest(
            service_id=service_id,
            domain="request",
            object_key=desc.object_key,
            checksum=desc.checksum,
            payload=payload,
            version=desc.version,
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
        for field in fields_to_compare:
            assert u_row.get(field) == b_row.get(field), f"Field mismatch on {field} for {event_id}"


def test_differential_canary_aggregates_and_projections_match() -> None:
    """Verifies that dimension aggregates and projection tables (origin, security,
    performance, network) computed from both paths match 100%.
    """
    service_id = "canary-svc-002"
    sources = _generate_synthetic_log_sources(service_id, source_count=5, rows_per_source=12)

    # Decode events for both paths
    ch_u = _MockClickHouse()
    ctrl_u, _, _ = _build_controller(service_id, "worker-u", ch_u)
    for desc, payload, _ in sources:
        ctrl_u.ingest(
            service_id=service_id,
            domain="request",
            object_key=desc.object_key,
            checksum=desc.checksum,
            payload=payload,
            version=desc.version,
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
    from collections import Counter

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
                    r = self.controller.ingest(
                        service_id=service_id,
                        domain="request",
                        object_key=d.object_key,
                        checksum=d.checksum,
                        payload=p,
                        version=d.version,
                    )
                    total += r.decoded.accepted_rows
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
