"""Typed archive and serving-watermark contracts for the high-scale plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ArchiveSourceObject:
    service_id: str
    domain: str
    object_key: str
    checksum: str
    size_bytes: int
    version: str | None = None


@dataclass(frozen=True)
class ArchiveArtifact:
    uri: str
    checksum: str
    size_bytes: int
    row_count: int
    byte_count: int
    canonical_digest: str
    schema_version: str
    transform_version: str


@dataclass(frozen=True)
class ArchiveManifest:
    manifest_id: str
    source: ArchiveSourceObject
    artifact: ArchiveArtifact
    coverage_start: datetime
    coverage_end: datetime
    retention_deadline: datetime
    deletion_authorization_deadline: datetime
    archive_epoch: int

    def validate(self) -> None:
        if self.coverage_end < self.coverage_start:
            raise ValueError("archive coverage end precedes coverage start")
        if self.retention_deadline < self.coverage_end:
            raise ValueError("retention deadline precedes archive coverage")
        if self.deletion_authorization_deadline < self.retention_deadline:
            raise ValueError("deletion authorization precedes retention deadline")
        if self.archive_epoch < 0:
            raise ValueError("archive epoch must be non-negative")
        if self.artifact.row_count < 0 or self.artifact.byte_count < 0:
            raise ValueError("archive counts must be non-negative")


@dataclass(frozen=True)
class ServingWatermark:
    service_id: str
    domain: str
    owner_epoch: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    last_accepted_cursor: str | None
    last_archived_event_id: str | None
    last_visible_event_id: str | None
    exact: bool

    def validate(self) -> None:
        if self.owner_epoch < 0:
            raise ValueError("owner epoch must be non-negative")
        if self.coverage_start and self.coverage_end and self.coverage_end < self.coverage_start:
            raise ValueError("watermark coverage end precedes coverage start")
