"""Fenced source-object controller for the high-scale ingestion boundary."""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.aggregate_writer import compute_dimension_counts
from backend.high_scale.archive_models import ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import write_archive_checkpoint
from backend.high_scale.decoder import DecodeResult, decode_source_object
from backend.high_scale.ledger import HighScaleLedger, SourceObject
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    PublicationResult,
    PublicationStatus,
    row_matches_serving_domain,
)

logger = logging.getLogger(__name__)


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

    def archive_manifest(self, manifest_id: str) -> Any: ...

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
        aggregates: Any | None = None,
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
        self._aggregates = aggregates
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

        # From here on, use source.domain (the ledger's persisted domain
        # attribution for this object_key), never the caller's own `domain`
        # argument. Some domain pairs list the same FOS prefix (rum_vitals/
        # rum_errors both list raw/rum/), so the same not-yet-terminal
        # object can legitimately be re-attempted by a sibling domain's page
        # before this one finishes it. discover_source/ledger.discover
        # accept that (only a real checksum/version change is rejected) and
        # keep the FIRST domain's attribution — using the caller's own
        # domain instead here made every such retry fail downstream with
        # "source identity does not match archive batch", observed live
        # during RUM qualification.
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
            domain=source.domain,
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
                domain=source.domain,
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
            # manifest_id is content-addressed from (service_id, domain,
            # object_key, checksum, artifact_checksum) — stable across
            # retries — but coverage_start/coverage_end come from `observed`
            # (wall-clock `now`), which is NOT stable. A retry of an object
            # that already registered a manifest on a prior attempt
            # therefore rebuilds the SAME manifest_id with DIFFERENT
            # coverage fields. Reuse the already-registered manifest's
            # content instead of the freshly-built one so this attempt is
            # byte-identical to the original — both the archive store's
            # immutability contract and register_archive_manifest's own
            # identity check depend on that. Only a genuinely first attempt
            # (no existing record) uses the freshly-built manifest.
            if self._control is not None:
                try:
                    manifest = self._control.archive_manifest(manifest.manifest_id).manifest
                except KeyError:
                    pass
            self._archive.publish(manifest, artifact)
        if self._control is not None:
            self._publish_control_manifest(manifest, owner.owner_epoch)

        current_owner = (
            self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        )
        if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
            raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")
        serving_rows = tuple(e for e in decoded.events if row_matches_serving_domain(source.domain, e))
        if serving_rows:
            batch = HighScaleBatch(
                batch_id=f"{service_id}:{source.domain}:{source.object_id}",
                service_id=service_id,
                domain=source.domain,
                generation=str(owner.owner_epoch),
                rows=serving_rows,
            )
            publication = self._serving.publish(batch)
        else:
            # Every decoded row in this object belongs to a sibling domain
            # that shares the same raw prefix (rum_vitals/rum_errors both
            # list raw/rum/) — nothing here is shaped for source.domain.
            # The object is still archived above; there's simply nothing
            # to publish to this domain's fact table.
            publication = PublicationResult(
                batch_id=f"{service_id}:{source.domain}:{source.object_id}",
                status=PublicationStatus.VISIBLE,
                rows_visible=0,
                duplicate=False,
            )
        self._publish_aggregates(service_id, source, owner.owner_epoch, decoded.events)
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

    def _publish_aggregates(
        self,
        service_id: str,
        source: SourceObject,
        owner_epoch: int,
        events: tuple[dict[str, Any], ...],
    ) -> None:
        """Best-effort triage aggregates layered on the facts path: a
        failure here must never fail, retry, or duplicate the source
        object's ingest — the facts write above is already durably
        committed by the time this runs."""
        if self._aggregates is None:
            return
        try:
            counts = compute_dimension_counts(source.domain, events)
            if counts:
                batch = HighScaleBatch(
                    batch_id=f"{service_id}:{source.domain}_aggregate:{source.object_id}",
                    service_id=service_id,
                    domain=f"{source.domain}_aggregate",
                    generation=str(owner_epoch),
                    rows=counts,
                )
                self._aggregates.publish(batch)
            if source.domain == "request":
                projection = build_origin_projection_rows(events)
                for domain, rows in (
                    ("origin_summary", projection.summary_rows),
                    ("origin_dimensions", projection.dimension_rows),
                ):
                    if not rows:
                        continue
                    self._aggregates.publish(
                        HighScaleBatch(
                            batch_id=f"{service_id}:{domain}:{source.object_id}",
                            service_id=service_id,
                            domain=domain,
                            generation=str(owner_epoch),
                            rows=rows,
                        )
                    )
        except Exception:
            logger.exception(
                "high-scale aggregate publish failed",
                extra={"service_id": service_id, "domain": source.domain, "object_key": source.object_key},
            )

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
