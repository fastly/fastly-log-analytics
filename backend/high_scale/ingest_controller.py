"""Fenced source-object controller for the high-scale ingestion boundary."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from backend.high_scale.archive_models import ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import write_archive_checkpoint
from backend.high_scale.decoder import DecodeResult, decode_source_object
from backend.high_scale.ledger import HighScaleLedger, SourceObject
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import ClickHousePublication, HighScaleBatch, PublicationResult


@dataclass(frozen=True)
class IngestResult:
    source: SourceObject
    decoded: DecodeResult
    manifest: ArchiveManifest
    publication: PublicationResult


class HighScaleIngestController:
    """Coordinates one idempotent, owner-fenced source-object ingest."""

    def __init__(
        self,
        *,
        ownership: OwnershipStore,
        ledger: HighScaleLedger,
        archive: ArchivePublication,
        serving: ClickHousePublication,
        worker_id: str,
        transform_version: str = "normalize.v1",
        schema_version: str = "archive.v1",
        archive_epoch: int = 0,
        retention_seconds: int = 0,
        deletion_grace_seconds: int = 900,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        self._ownership = ownership
        self._ledger = ledger
        self._archive = archive
        self._serving = serving
        self._worker_id = worker_id
        self._transform_version = transform_version
        self._schema_version = schema_version
        self._archive_epoch = archive_epoch
        self._retention_seconds = retention_seconds
        self._deletion_grace_seconds = deletion_grace_seconds

    def ingest(
        self,
        *,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        payload: bytes,
        size_bytes: int | None = None,
        version: str | None = None,
        now: datetime | None = None,
    ) -> IngestResult:
        owner = self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest is not the owner for {service_id}")
        source = self._ledger.discover(
            service_id,
            domain,
            object_key,
            checksum,
            size_bytes=len(payload) if size_bytes is None else size_bytes,
            version=version,
        )
        claim = self._ledger.claim(object_key, self._worker_id)
        if not claim.claimed:
            raise RuntimeError(f"source object is leased by another worker: {object_key}")

        archive_source = ArchiveSourceObject(
            source.service_id,
            source.domain,
            source.object_key,
            source.checksum,
            source.size_bytes,
            source.version,
        )
        decoded = decode_source_object(
            archive_source,
            payload,
            transform_version=self._transform_version,
            domain=domain,
        )
        self._ledger.record_counts(
            object_key,
            accepted_rows=decoded.accepted_rows,
            malformed_rows=decoded.quarantined_rows,
        )
        if not decoded.events:
            raise ValueError("source object contains no accepted records")

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        with tempfile.TemporaryDirectory(prefix="high-scale-archive-") as root:
            local_manifest = write_archive_checkpoint(
                Path(root),
                service_id=service_id,
                domain=domain,
                source=archive_source,
                events=list(decoded.events),
                archive_epoch=self._archive_epoch,
                schema_version=self._schema_version,
                transform_version=self._transform_version,
                coverage_start=observed,
                coverage_end=observed,
                retention_seconds=self._retention_seconds,
                deletion_grace_seconds=self._deletion_grace_seconds,
            )
            artifact_path = Path(local_manifest.artifact.uri.removeprefix("file://"))
            artifact = artifact_path.read_bytes()
            manifest = replace(
                local_manifest,
                artifact=replace(
                    local_manifest.artifact,
                    uri=f"s3://archive/{local_manifest.manifest_id}.parquet",
                ),
            )
            self._archive.publish(manifest, artifact)

        batch = HighScaleBatch(
            batch_id=f"{service_id}:{domain}:{source.object_id}",
            service_id=service_id,
            domain=domain,
            generation=str(owner.owner_epoch),
            rows=tuple(decoded.events),
        )
        publication = self._serving.publish(batch)
        self._ledger.mark_appended(object_key, claim.lease_generation)
        self._ledger.mark_archived(
            object_key,
            claim.lease_generation,
            manifest.manifest_id,
            owner.owner_epoch,
        )
        self._ledger.acknowledge(object_key, manifest.manifest_id)
        return IngestResult(source, decoded, manifest, publication)
