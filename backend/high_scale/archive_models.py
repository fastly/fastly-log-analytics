"""Typed archive and serving-watermark contracts for the high-scale plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


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
class ArchiveSourceEntry:
    key: str
    checksum: str
    size_bytes: int
    row_start: int
    row_count: int
    deletion_deadline: datetime
    version: str | None = None


@dataclass(frozen=True, init=False)
class ArchiveManifest:
    manifest_id: str
    artifact: ArchiveArtifact
    coverage_start: datetime
    coverage_end: datetime
    retention_deadline: datetime
    deletion_authorization_deadline: datetime
    archive_epoch: int
    service_id: str
    domain: str
    sources: tuple[ArchiveSourceEntry, ...]
    source: ArchiveSourceObject

    def __init__(
        self,
        manifest_id: str,
        source: ArchiveSourceObject | None = None,
        artifact: ArchiveArtifact | None = None,
        coverage_start: datetime | None = None,
        coverage_end: datetime | None = None,
        retention_deadline: datetime | None = None,
        deletion_authorization_deadline: datetime | None = None,
        archive_epoch: int | None = None,
        *,
        service_id: str | None = None,
        domain: str | None = None,
        sources: tuple[ArchiveSourceEntry, ...] | list[ArchiveSourceEntry] | None = None,
        **kwargs: Any,
    ) -> None:
        if artifact is None and "artifact" in kwargs:
            artifact = kwargs["artifact"]
        if coverage_start is None and "coverage_start" in kwargs:
            coverage_start = kwargs["coverage_start"]
        if coverage_end is None and "coverage_end" in kwargs:
            coverage_end = kwargs["coverage_end"]
        if retention_deadline is None and "retention_deadline" in kwargs:
            retention_deadline = kwargs["retention_deadline"]
        if deletion_authorization_deadline is None and "deletion_authorization_deadline" in kwargs:
            deletion_authorization_deadline = kwargs["deletion_authorization_deadline"]
        if archive_epoch is None and "archive_epoch" in kwargs:
            archive_epoch = kwargs["archive_epoch"]

        if (
            artifact is None
            or coverage_start is None
            or coverage_end is None
            or retention_deadline is None
            or deletion_authorization_deadline is None
            or archive_epoch is None
        ):
            raise TypeError("ArchiveManifest missing required artifact, coverage, or retention parameters")

        if sources is not None:
            resolved_sources = tuple(sources)
        elif source is not None:
            resolved_sources = (
                ArchiveSourceEntry(
                    key=source.object_key,
                    checksum=source.checksum,
                    size_bytes=source.size_bytes,
                    row_start=0,
                    row_count=artifact.row_count,
                    deletion_deadline=deletion_authorization_deadline,
                    version=source.version,
                ),
            )
        else:
            resolved_sources = ()

        resolved_service_id = service_id or (source.service_id if source else "")
        resolved_domain = domain or (source.domain if source else "request")

        resolved_source = source
        if resolved_source is None and resolved_sources and resolved_service_id:
            first = resolved_sources[0]
            resolved_source = ArchiveSourceObject(
                service_id=resolved_service_id,
                domain=resolved_domain,
                object_key=first.key,
                checksum=first.checksum,
                size_bytes=first.size_bytes,
                version=first.version,
            )

        if resolved_source is None:
            raise TypeError("ArchiveManifest requires either source or non-empty sources")

        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "coverage_start", coverage_start)
        object.__setattr__(self, "coverage_end", coverage_end)
        object.__setattr__(self, "retention_deadline", retention_deadline)
        object.__setattr__(self, "deletion_authorization_deadline", deletion_authorization_deadline)
        object.__setattr__(self, "archive_epoch", archive_epoch)
        object.__setattr__(self, "service_id", resolved_service_id)
        object.__setattr__(self, "domain", resolved_domain)
        object.__setattr__(self, "sources", resolved_sources)
        object.__setattr__(self, "source", resolved_source)

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
        total_source_rows = sum(s.row_count for s in self.sources)
        if total_source_rows > self.artifact.row_count:
            raise ValueError(
                f"total source row count {total_source_rows} exceeds artifact row_count {self.artifact.row_count}"
            )

    def get_source_slice(self, object_key: str) -> tuple[int, int]:
        for s in self.sources:
            if s.key == object_key:
                return (s.row_start, s.row_start + s.row_count)
        raise KeyError(f"source key {object_key} not found in manifest")

    def is_source_eligible_for_deletion(self, object_key: str, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.deletion_authorization_deadline.tzinfo)
        for s in self.sources:
            if s.key == object_key:
                return now >= s.deletion_deadline
        raise KeyError(f"source key {object_key} not found in manifest")


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
