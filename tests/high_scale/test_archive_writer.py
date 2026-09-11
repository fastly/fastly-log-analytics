from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ArchiveSourceObject
from backend.high_scale.archive_writer import verify_archive_checkpoint, write_archive_checkpoint


def test_archive_checkpoint_is_verified_and_replayable(tmp_path) -> None:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:a", 12, "v1")
    manifest = write_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="request",
        source=source,
        events=[{"event_id": "1", "status": 200}, {"event_id": "2", "status": 500}],
        archive_epoch=4,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        retention_seconds=3600,
        deletion_grace_seconds=900,
    )
    verify_archive_checkpoint(manifest)
    assert manifest.artifact.row_count == 2
    assert manifest.deletion_authorization_deadline > manifest.retention_deadline
    assert (tmp_path / f"{manifest.manifest_id}.manifest.json").is_file()


def test_archive_checkpoint_rejects_mismatched_source(tmp_path) -> None:
    source = ArchiveSourceObject("other", "request", "raw/request/a.gz", "sha256:a", 12)
    with pytest.raises(ValueError, match="source identity"):
        write_archive_checkpoint(
            tmp_path,
            service_id="svc",
            domain="request",
            source=source,
            events=[{"event_id": "1"}],
            archive_epoch=1,
            schema_version="request.v1",
            transform_version="normalize.v1",
            coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        )


def test_archive_checksum_failure_is_detected(tmp_path) -> None:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:a", 12)
    manifest = write_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="request",
        source=source,
        events=[{"event_id": "1"}],
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )
    artifact_path = next(tmp_path.glob("*.parquet"))
    artifact_path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        verify_archive_checkpoint(manifest)
