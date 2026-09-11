import hashlib
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import (
    ArchivePublication,
    InMemoryObjectStore,
    PublicationState,
)


def _manifest() -> ArchiveManifest:
    return ArchiveManifest(
        manifest_id="manifest-1",
        source=ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 10, "v1"),
        artifact=ArchiveArtifact(
            uri="s3://archive/artifacts/manifest-1.parquet",
            checksum="sha256:artifact",
            size_bytes=4,
            row_count=2,
            byte_count=20,
            canonical_digest="sha256:events",
            schema_version="request.v1",
            transform_version="normalize.v1",
        ),
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        retention_deadline=datetime(2026, 9, 2, tzinfo=UTC),
        deletion_authorization_deadline=datetime(2026, 9, 3, tzinfo=UTC),
        archive_epoch=4,
    )


def test_publication_requires_verified_artifact_before_manifest_commit() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = _manifest()

    with pytest.raises(ValueError, match="artifact checksum"):
        publication.publish(manifest, b"bad")

    assert publication.state(manifest.manifest_id) is None
    assert not store.exists("archive/manifests/manifest-1.commit")


def test_publication_commits_manifest_only_after_verified_artifact() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = _manifest()
    artifact = b"data"

    manifest = replace(manifest, artifact=replace(manifest.artifact, checksum=_checksum(artifact)))

    result = publication.publish(manifest, artifact)

    assert result.state is PublicationState.MANIFEST_COMMITTED
    assert result.manifest_id == manifest.manifest_id
    assert store.exists("archive/manifests/manifest-1.commit")
    assert publication.is_replayable(manifest.manifest_id)


def test_commit_marker_digest_mismatch_is_not_replayable() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = replace(_manifest(), artifact=replace(_manifest().artifact, checksum=_checksum(b"data")))
    publication.publish(manifest, b"data")
    store.put("archive/manifests/manifest-1.commit", b"tampered")

    assert publication.is_replayable(manifest.manifest_id) is False


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"
