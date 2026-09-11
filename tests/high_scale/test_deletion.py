import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
from backend.high_scale.ledger import HighScaleLedger


def test_source_deletion_requires_archive_and_owner_fence() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    ledger = HighScaleLedger()
    try:
        source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
        ledger.discover("svc", "request", source.object_key, source.checksum, size_bytes=4, version="v1")
        claim = ledger.claim(source.object_key, "worker")
        ledger.record_counts(source.object_key, accepted_rows=1, malformed_rows=0)
        ledger.mark_appended(source.object_key, claim.lease_generation)
        now = datetime(2026, 9, 4, tzinfo=UTC)
        manifest = ArchiveManifest(
            "manifest-1",
            source,
            ArchiveArtifact(
                "s3://archive/artifacts/manifest-1.parquet",
                _checksum(b"data"),
                4,
                1,
                4,
                "sha256:events",
                "request.v1",
                "normalize.v1",
            ),
            now - timedelta(days=3),
            now - timedelta(days=2),
            now - timedelta(days=1),
            now - timedelta(hours=1),
            3,
        )
        artifact = b"data"
        store.put(source.object_key, b"raw")
        publication.publish(manifest, artifact)
        ledger.mark_archived(source.object_key, claim.lease_generation, manifest.manifest_id, 3)
        ledger.acknowledge(source.object_key, manifest.manifest_id)

        DeletionController(store, publication, ledger).delete_source(manifest, current_owner_epoch=3, now=now)

        assert not store.exists(source.object_key)
    finally:
        ledger.close()


def test_source_deletion_is_blocked_before_deadline() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    ledger = HighScaleLedger()
    try:
        with pytest.raises(ValueError, match="grace period"):
            DeletionController(store, publication, ledger).delete_source(
                _manifest(), current_owner_epoch=3, now=datetime(2026, 9, 1, tzinfo=UTC)
            )
    finally:
        ledger.close()


def _manifest() -> ArchiveManifest:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    return ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact("s3://archive/artifacts/manifest-1.parquet", "sha256:x", 1, 1, 1, "sha256:e", "v1", "v1"),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        3,
    )


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"
