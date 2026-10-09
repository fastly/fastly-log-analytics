"""Archive-only recovery test suite for the High-Scale ingestion redesign.

Validates ADR-21 and High-Scale Design §9.8 / §11 Phase 3.3 requirements:
1. Rebuild ClickHouse serving state directly from FOS archive artifacts using
   the batch manifest format without requiring any raw landing log files.
2. Replay origin projection rows directly from archived batch manifests.
3. Verify idempotency of replay attempts and custom rebuild scope isolation.
4. Verify catalog-level recovery across recent and warm history tiers.
"""

from __future__ import annotations

import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from backend.high_scale.archive_models import (
    ArchiveManifest,
    ArchiveSourceObject,
)
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.archive_writer import write_batch_archive_checkpoint
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PublicationStatus,
)
from backend.high_scale.recovery import (
    rebuild_origin_projections_from_fos,
    rebuild_serving_state_from_fos,
)
from backend.high_scale.replay import replay_manifest, replay_origin_projections


class _MockClickHouseClient:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    @property
    def request_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for b in self.batches:
            if b.domain == "request":
                rows.extend(b.rows)
        return rows


class _InMemoryCatalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]:
        return tuple(
            m
            for m in self._manifests
            if m.service_id == service_id and m.domain == domain and m.coverage_end > start and m.coverage_start < end
        )


def _build_synthetic_batch_archive(
    service_id: str,
    output_dir: Path,
    source_count: int = 5,
    rows_per_source: int = 5,
    now: datetime | None = None,
) -> tuple[ArchiveManifest, bytes, list[dict[str, Any]]]:
    observed = now or datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    source_batches: list[tuple[ArchiveSourceObject, tuple[dict[str, Any], ...]]] = []
    all_events: list[dict[str, Any]] = []

    for s_idx in range(source_count):
        events_for_source: list[dict[str, Any]] = []
        for r_idx in range(rows_per_source):
            event = {
                "event_id": f"evt-{service_id}-{s_idx}-{r_idx}",
                "timestamp": (observed - timedelta(minutes=s_idx, seconds=r_idx * 10)).isoformat(),
                "client_ip": f"198.51.100.{(s_idx * 5 + r_idx) % 250 + 1}",
                "request_method": "GET" if r_idx % 2 == 0 else "POST",
                "url": f"/api/v1/item/{s_idx}_{r_idx}",
                "status": 200 if r_idx != 3 else 500,
                "ost": 200 if r_idx != 3 else 500,
                "ottfb": 45000.0,
                "ttfb": 0.045,
                "cache": "MISS",
                "resp_bytes": 1024 * (r_idx + 1),
                "pop": "JFK" if s_idx % 2 == 0 else "SFO",
                "fastly_backend_name": "origin-primary",
                "time_elapsed": 30000 + (r_idx * 500),
            }
            events_for_source.append(event)
            all_events.append(event)

        source_obj = ArchiveSourceObject(
            service_id=service_id,
            domain="request",
            object_key=f"raw/request/year=2026/month=10/day=09/source_{s_idx:03d}.gz",
            checksum=f"sha256:synth_{s_idx:04d}",
            size_bytes=512,
            version=f"v{s_idx}",
        )
        source_batches.append((source_obj, tuple(events_for_source)))

    manifest = write_batch_archive_checkpoint(
        output_dir,
        service_id=service_id,
        domain="request",
        source_batches=source_batches,
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=observed - timedelta(hours=1),
        coverage_end=observed,
        retention_seconds=86400 * 30,
        deletion_grace_seconds=86400 * 7,
    )

    artifact_path = Path(manifest.artifact.uri.removeprefix("file://"))
    artifact_bytes = artifact_path.read_bytes()
    normalized_manifest = replace(
        manifest,
        artifact=replace(
            manifest.artifact,
            uri=f"s3://archive/artifacts/{manifest.manifest_id}.parquet",
        ),
    )
    return normalized_manifest, artifact_bytes, all_events


def test_archive_only_recovery_rebuilds_all_events_without_raw_logs() -> None:
    """Verifies that a lost ClickHouse table can be completely rebuilt directly
    from FOS archive artifacts using the batch manifest format, without touching
    or requiring the original raw Fastly .gz log files.
    """
    service_id = "recover-svc-001"
    now = datetime(2026, 10, 9, 14, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, expected_events = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    # 1. Publish to FOS archive store
    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    pub_res = archive.publish(manifest, artifact_bytes)
    assert archive.is_replayable(manifest.manifest_id)

    # 2. Simulate complete absence/deletion of edge raw logs
    # No raw .gz files exist or are referenced anywhere in the recovery path.

    # 3. Simulate a fresh ClickHouse cluster after state wipe
    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    # 4. Replay the batch manifest
    replay_res = replay_manifest(manifest, archive, publication)

    assert replay_res.manifest_id == manifest.manifest_id
    assert replay_res.rows_read == 25
    assert replay_res.publication.rows_visible == 25
    assert replay_res.publication.status == PublicationStatus.VISIBLE
    assert replay_res.publication.duplicate is False

    # 5. Assert all 25 rows recovered with accurate attributes
    recovered_rows = ch_client.request_rows
    assert len(recovered_rows) == 25

    recovered_ids = {r["event_id"] for r in recovered_rows}
    expected_ids = {e["event_id"] for e in expected_events}
    assert recovered_ids == expected_ids


def test_archive_only_recovery_rebuilds_origin_projections() -> None:
    """Verifies that origin projections (summary and dimensions) can be fully
    rebuilt from an archived batch manifest.
    """
    service_id = "recover-svc-002"
    now = datetime(2026, 10, 9, 14, 30, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(manifest, artifact_bytes)

    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    proj_res = replay_origin_projections(
        manifest,
        archive,
        publication,
        rebuild_id="drill-origin-001",
    )

    assert proj_res.rows_read == 25
    assert proj_res.rows_published > 0

    origin_batches = [b for b in ch_client.batches if b.domain in {"origin_summary", "origin_dimensions"}]
    assert len(origin_batches) >= 1
    total_proj_rows = sum(len(b.rows) for b in origin_batches)
    assert total_proj_rows == proj_res.rows_published


def test_archive_only_recovery_idempotency_and_rebuild_scopes() -> None:
    """Verifies that:
    1. A duplicate replay with default batch ID is correctly recognized as duplicate.
    2. A disaster-recovery replay scoped with a fresh rebuild ID successfully re-inserts.
    """
    service_id = "recover-svc-003"
    now = datetime(2026, 10, 9, 15, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(manifest, artifact_bytes)

    inserted_batches: list[HighScaleBatch] = []

    class CountingClient:
        def insert(self, batch: HighScaleBatch) -> InsertReceipt:
            inserted_batches.append(batch)
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, CountingClient())

    # First attempt: Fresh insert
    res1 = replay_manifest(manifest, archive, publication)
    assert res1.publication.duplicate is False
    assert len(inserted_batches) == 1

    # Second attempt: Duplicate with default batch ID
    res2 = replay_manifest(manifest, archive, publication)
    assert res2.publication.duplicate is True
    assert len(inserted_batches) == 1

    # Third attempt: Rebuild-scoped batch ID (disaster recovery after volume wipe)
    rebuild_batch_id = f"rebuild:disaster-drill-99:{manifest.manifest_id}"
    res3 = replay_manifest(manifest, archive, publication, batch_id=rebuild_batch_id)
    assert res3.publication.duplicate is False
    assert len(inserted_batches) == 2


def test_archive_only_recovery_catalog_tiered_rebuild() -> None:
    """Verifies full catalog recovery across recent and warm history tiers
    using batch manifests and rebuild_serving_state_from_fos.
    """
    service_id = "recover-svc-004"
    now = datetime(2026, 10, 9, 16, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        recent_manifest, recent_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=3,
            rows_per_source=10,
            now=now - timedelta(hours=2),
        )
        warm_manifest, warm_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=4,
            rows_per_source=10,
            now=now - timedelta(days=5),
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(recent_manifest, recent_bytes)
    archive.publish(warm_manifest, warm_bytes)

    catalog = _InMemoryCatalog((recent_manifest, warm_manifest))
    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    report = rebuild_serving_state_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        domain="request",
        now=now,
    )

    assert report.full_rebuild_complete is True
    assert report.manifests_replayed == 2
    assert report.rows_replayed == 70  # (3*10) + (4*10)
    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is True
    assert len(report.errors) == 0

    # Also test origin projection catalog rebuild
    proj_report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        now=now,
    )
    assert proj_report.full_rebuild_complete is True
    assert proj_report.manifests_replayed == 2
    assert proj_report.rows_replayed > 0
