"""Fenced source-object controller for the high-scale ingestion boundary."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from backend.core.high_scale_contracts import ArchiveState
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


class HighScaleControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...

    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int,
        version: str | None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> Any: ...

    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float,
        now: datetime | None,
    ) -> Any: ...

    def record_source_counts(
        self,
        service_id: str,
        object_key: str,
        *,
        accepted_rows: int,
        malformed_rows: int,
        expected_owner_epoch: int,
    ) -> None: ...

    def register_archive_manifest(self, manifest: ArchiveManifest, *, owner_epoch: int) -> Any: ...

    def transition_archive_manifest(
        self,
        manifest_id: str,
        *,
        expected_state: ArchiveState,
        next_state: ArchiveState,
        expected_owner_epoch: int,
    ) -> Any: ...

    def mark_source_appended(
        self,
        service_id: str,
        object_key: str,
        *,
        lease_generation: int,
        expected_owner_epoch: int,
    ) -> None: ...

    def mark_source_archived(
        self,
        service_id: str,
        object_key: str,
        *,
        lease_generation: int,
        manifest_id: str,
        owner_epoch: int,
    ) -> None: ...

    def acknowledge_source(self, service_id: str, object_key: str, *, manifest_id: str) -> None: ...


class HighScaleIngestController:
    """Coordinates one idempotent, owner-fenced source-object ingest."""

    def __init__(
        self,
        *,
        ownership: OwnershipStore,
        ledger: HighScaleLedger,
        control_plane: HighScaleControlPlane | None = None,
        archive: ArchivePublication,
        serving: ClickHousePublication,
        worker_id: str,
        transform_version: str = "normalize.v1",
        schema_version: str = "archive.v1",
        archive_epoch: int = 0,
        retention_seconds: int = 0,
        deletion_grace_seconds: int = 900,
        lease_seconds: float = 300.0,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if lease_seconds <= 0:
            raise ValueError("lease duration must be positive")
        self._ownership = ownership
        self._ledger = ledger
        self._control = control_plane
        self._archive = archive
        self._serving = serving
        self._worker_id = worker_id
        self._transform_version = transform_version
        self._schema_version = schema_version
        self._archive_epoch = archive_epoch
        self._retention_seconds = retention_seconds
        self._deletion_grace_seconds = deletion_grace_seconds
        self._lease_seconds = lease_seconds

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
        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest is not the owner for {service_id}")
        source = (
            self._control.discover_source(
                service_id,
                domain,
                object_key,
                checksum,
                size_bytes=len(payload) if size_bytes is None else size_bytes,
                version=version,
                expected_owner=owner.current_owner,
                expected_owner_epoch=owner.owner_epoch,
            )
            if self._control is not None
            else self._ledger.discover(
                service_id,
                domain,
                object_key,
                checksum,
                size_bytes=len(payload) if size_bytes is None else size_bytes,
                version=version,
            )
        )
        claim = (
            self._control.claim_source(
                service_id,
                object_key,
                self._worker_id,
                expected_owner=owner.current_owner,
                expected_owner_epoch=owner.owner_epoch,
                lease_seconds=self._lease_seconds,
                now=now,
            )
            if self._control is not None
            else self._ledger.claim(service_id, object_key, self._worker_id)
        )
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
        if self._control is not None:
            self._control.record_source_counts(
                service_id,
                object_key,
                accepted_rows=decoded.accepted_rows,
                malformed_rows=decoded.quarantined_rows,
                expected_owner_epoch=owner.owner_epoch,
            )
        else:
            self._ledger.record_counts(
                service_id,
                object_key,
                accepted_rows=decoded.accepted_rows,
                malformed_rows=decoded.quarantined_rows,
            )
        if not decoded.events and not decoded.dead_letters:
            raise ValueError("source object contains no accepted records or dead letters")
        archive_rows = [dict(event, _record_kind="event") for event in decoded.events]
        archive_rows.extend(
            {
                "_record_kind": "dead_letter",
                "source_object_key": item.source_object_key,
                "line_ordinal": item.line_ordinal,
                "raw_line_base64": item.raw_line_base64,
                "reason": item.reason,
            }
            for item in decoded.dead_letters
        )

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        with tempfile.TemporaryDirectory(prefix="high-scale-archive-") as root:
            local_manifest = write_archive_checkpoint(
                Path(root),
                service_id=service_id,
                domain=domain,
                source=archive_source,
                events=archive_rows,
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
        if self._control is not None:
            self._publish_control_manifest(manifest, owner.owner_epoch)

        current_owner = (
            self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        )
        if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
            raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")
        batch = HighScaleBatch(
            batch_id=f"{service_id}:{domain}:{source.object_id}",
            service_id=service_id,
            domain=domain,
            generation=str(owner.owner_epoch),
            rows=tuple(decoded.events),
        )
        publication = self._serving.publish(batch)
        if self._control is not None:
            self._control.mark_source_appended(
                service_id,
                object_key,
                lease_generation=claim.lease_generation,
                expected_owner_epoch=owner.owner_epoch,
            )
            self._control.mark_source_archived(
                service_id,
                object_key,
                lease_generation=claim.lease_generation,
                manifest_id=manifest.manifest_id,
                owner_epoch=owner.owner_epoch,
            )
            self._control.acknowledge_source(service_id, object_key, manifest_id=manifest.manifest_id)
        else:
            self._ledger.mark_appended(service_id, object_key, claim.lease_generation)
            self._ledger.mark_archived(
                service_id,
                object_key,
                claim.lease_generation,
                manifest.manifest_id,
                owner.owner_epoch,
            )
            self._ledger.acknowledge(service_id, object_key, manifest.manifest_id)
        return IngestResult(source, decoded, manifest, publication)

    def _publish_control_manifest(self, manifest: ArchiveManifest, owner_epoch: int) -> None:
        assert self._control is not None
        record = self._control.register_archive_manifest(manifest, owner_epoch=owner_epoch)
        states = (
            ArchiveState.ARTIFACT_UPLOADING,
            ArchiveState.ARTIFACT_VERIFIED,
            ArchiveState.MANIFEST_PREPARED,
            ArchiveState.MANIFEST_COMMITTED,
        )
        state_index = states.index(record.state)
        for next_state in states[state_index + 1 :]:
            self._control.transition_archive_manifest(
                manifest.manifest_id,
                expected_state=states[state_index],
                next_state=next_state,
                expected_owner_epoch=owner_epoch,
            )
            state_index += 1
