from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
)


def _base_artifact(row_count: int = 100, byte_count: int = 5000) -> ArchiveArtifact:
    return ArchiveArtifact(
        uri="s3://test-bucket/high-scale/archive/manifest-1.parquet",
        checksum="sha256:artifact123",
        size_bytes=4096,
        row_count=row_count,
        byte_count=byte_count,
        canonical_digest="sha256:digest123",
        schema_version="request.v1",
        transform_version="normalize.v1",
    )


def test_batch_manifest_supports_n_sources_and_validates() -> None:
    now = datetime.now(UTC)
    sources = tuple(
        ArchiveSourceEntry(
            key=f"raw/request/2026-10-09/part_{i}.gz",
            checksum=f"sha256:chk_{i}",
            size_bytes=1024,
            version=f"v_{i}",
            row_start=i * 20,
            row_count=20,
            deletion_deadline=now + timedelta(days=7),
        )
        for i in range(5)
    )

    manifest = ArchiveManifest(
        manifest_id="manifest-batch-001",
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    # Validation should pass
    manifest.validate()
    assert len(manifest.sources) == 5
    assert manifest.sources[0].row_start == 0
    assert manifest.sources[0].row_count == 20
    assert manifest.sources[4].row_start == 80


def test_batch_manifest_rejects_row_overflow() -> None:
    now = datetime.now(UTC)
    sources = (
        ArchiveSourceEntry(
            key="raw/request/part_0.gz",
            checksum="sha256:chk_0",
            size_bytes=1024,
            row_start=0,
            row_count=60,
            deletion_deadline=now + timedelta(days=7),
        ),
        ArchiveSourceEntry(
            key="raw/request/part_1.gz",
            checksum="sha256:chk_1",
            size_bytes=1024,
            row_start=60,
            row_count=50,  # 60 + 50 = 110 > artifact row_count 100
            deletion_deadline=now + timedelta(days=7),
        ),
    )

    manifest = ArchiveManifest(
        manifest_id="manifest-batch-002",
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    with pytest.raises(ValueError, match="exceeds artifact row_count"):
        manifest.validate()


def test_per_source_replay_finds_row_range() -> None:
    now = datetime.now(UTC)
    sources = (
        ArchiveSourceEntry(
            key="raw/request/file1.gz",
            checksum="sha256:chk1",
            size_bytes=500,
            row_start=0,
            row_count=40,
            deletion_deadline=now + timedelta(days=7),
        ),
        ArchiveSourceEntry(
            key="raw/request/file2.gz",
            checksum="sha256:chk2",
            size_bytes=600,
            row_start=40,
            row_count=60,
            deletion_deadline=now + timedelta(days=7),
        ),
    )
    manifest = ArchiveManifest(
        manifest_id="manifest-batch-003",
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    # Replay slice lookup helper on manifest
    slice1 = manifest.get_source_slice("raw/request/file1.gz")
    assert slice1 == (0, 40)
    slice2 = manifest.get_source_slice("raw/request/file2.gz")
    assert slice2 == (40, 100)


def test_independent_per_source_deletion_deadlines() -> None:
    now = datetime.now(UTC)
    s1 = ArchiveSourceEntry(
        key="raw/request/file1.gz",
        checksum="sha256:chk1",
        size_bytes=500,
        row_start=0,
        row_count=50,
        deletion_deadline=now - timedelta(minutes=1),  # Eligible now
    )
    s2 = ArchiveSourceEntry(
        key="raw/request/file2.gz",
        checksum="sha256:chk2",
        size_bytes=600,
        row_start=50,
        row_count=50,
        deletion_deadline=now + timedelta(hours=1),  # Not eligible yet
    )
    manifest = ArchiveManifest(
        manifest_id="manifest-batch-004",
        service_id="svc_test",
        domain="request",
        sources=(s1, s2),
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    assert manifest.is_source_eligible_for_deletion("raw/request/file1.gz", now=now) is True
    assert manifest.is_source_eligible_for_deletion("raw/request/file2.gz", now=now) is False


def test_write_batch_archive_checkpoint_end_to_end(tmp_path) -> None:
    from backend.high_scale.archive_models import ArchiveSourceObject
    from backend.high_scale.archive_writer import (
        verify_archive_checkpoint,
        write_batch_archive_checkpoint,
    )

    now = datetime.now(UTC)
    s1 = ArchiveSourceObject("svc_test", "request", "raw/request/part_0.gz", "sha256:chk0", 1024)
    s2 = ArchiveSourceObject("svc_test", "request", "raw/request/part_1.gz", "sha256:chk1", 2048)

    events_1 = [{"event_id": f"s1_{i}", "status": 200, "client_ip": "1.1.1.1"} for i in range(15)]
    events_2 = [{"event_id": f"s2_{i}", "status": 404, "client_ip": "2.2.2.2"} for i in range(25)]

    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc_test",
        domain="request",
        source_batches=[(s1, events_1), (s2, events_2)],
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=now - timedelta(minutes=1),
        coverage_end=now,
    )

    assert len(manifest.sources) == 2
    assert manifest.sources[0].key == "raw/request/part_0.gz"
    assert manifest.sources[0].row_start == 0
    assert manifest.sources[0].row_count == 15
    assert manifest.sources[1].key == "raw/request/part_1.gz"
    assert manifest.sources[1].row_start == 15
    assert manifest.sources[1].row_count == 25
    assert manifest.artifact.row_count == 40

    # Verification must succeed
    verify_archive_checkpoint(manifest)
