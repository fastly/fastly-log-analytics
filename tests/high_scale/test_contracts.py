import pytest

from backend.core.high_scale_contracts import (
    ArchiveManifest,
    ArchiveState,
    validate_archive_transition,
)


def test_archive_states_are_monotonic_and_manifest_fields_are_required() -> None:
    validate_archive_transition(ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED)
    with pytest.raises(ValueError, match="monotonic"):
        validate_archive_transition(ArchiveState.MANIFEST_COMMITTED, ArchiveState.MANIFEST_PREPARED)

    manifest = ArchiveManifest(
        service_id="service",
        domain="request",
        source_key="raw/request/file.gz",
        source_checksum="sha256:source",
        source_size=10,
        source_version="version",
        artifact_uri="s3://bucket/archive/file.parquet",
        artifact_checksum="sha256:artifact",
        artifact_size=20,
        row_count=2,
        byte_count=100,
        domain_counts={"request": 2},
        schema_version="request.v1",
        transform_version="normalize.v1",
        event_digest="sha256:events",
        retention_deadline="2026-10-01T00:00:00+00:00",
        deletion_authorization_deadline="2026-10-02T00:00:00+00:00",
        archive_epoch=7,
    )
    assert manifest.validate() is None


def test_archive_deletion_requires_committed_manifest_and_matching_epoch() -> None:
    manifest = ArchiveManifest(
        service_id="service",
        domain="request",
        source_key="raw/request/file.gz",
        source_checksum="sha256:source",
        source_size=10,
        source_version="version",
        artifact_uri="s3://bucket/archive/file.parquet",
        artifact_checksum="sha256:artifact",
        artifact_size=20,
        row_count=2,
        byte_count=100,
        domain_counts={"request": 2},
        schema_version="request.v1",
        transform_version="normalize.v1",
        event_digest="sha256:events",
        retention_deadline="2026-10-01T00:00:00+00:00",
        deletion_authorization_deadline="2026-10-02T00:00:00+00:00",
        archive_epoch=7,
    )
    assert manifest.can_delete(
        state=ArchiveState.DELETION_ELIGIBLE,
        current_archive_epoch=7,
        replay_lease_active=False,
    )
    assert not manifest.can_delete(
        state=ArchiveState.MANIFEST_COMMITTED,
        current_archive_epoch=7,
        replay_lease_active=False,
    )
    assert not manifest.can_delete(
        state=ArchiveState.DELETION_ELIGIBLE,
        current_archive_epoch=8,
        replay_lease_active=False,
    )
    assert not manifest.can_delete(
        state=ArchiveState.DELETION_ELIGIBLE,
        current_archive_epoch=7,
        replay_lease_active=True,
    )
