"""Contracts for the high-scale data plane.

This module records the ownership and safety boundary that the high-scale
implementation satisfies.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ArchiveState(StrEnum):
    ARTIFACT_UPLOADING = "artifact_uploading"
    ARTIFACT_VERIFIED = "artifact_verified"
    MANIFEST_PREPARED = "manifest_prepared"
    MANIFEST_COMMITTED = "manifest_committed"
    DELETION_ELIGIBLE = "deletion_eligible"
    SOURCE_DELETED = "source_deleted"


ARCHIVE_STATES = tuple(ArchiveState)


def validate_archive_transition(current: ArchiveState, next_state: ArchiveState) -> None:
    """Reject state regressions and transitions that skip a protocol phase."""
    try:
        current_index = ARCHIVE_STATES.index(current)
        next_index = ARCHIVE_STATES.index(next_state)
    except ValueError as exc:
        raise ValueError("unknown archive state") from exc
    if next_index != current_index + 1:
        raise ValueError("archive state transitions must be monotonic and adjacent")


@dataclass(frozen=True)
class ArchiveManifest:
    service_id: str
    domain: str
    source_key: str
    source_checksum: str
    source_size: int
    source_version: str
    artifact_uri: str
    artifact_checksum: str
    artifact_size: int
    row_count: int
    byte_count: int
    domain_counts: dict[str, int]
    schema_version: str
    transform_version: str
    event_digest: str
    retention_deadline: str
    deletion_authorization_deadline: str
    archive_epoch: int

    def validate(self) -> None:
        required_strings = (
            self.service_id,
            self.domain,
            self.source_key,
            self.source_checksum,
            self.source_version,
            self.artifact_uri,
            self.artifact_checksum,
            self.schema_version,
            self.transform_version,
            self.event_digest,
            self.retention_deadline,
            self.deletion_authorization_deadline,
        )
        if not all(required_strings):
            raise ValueError("archive manifest identity and version fields are required")
        if self.source_size < 0 or self.artifact_size < 0 or self.row_count < 0 or self.byte_count < 0:
            raise ValueError("archive manifest sizes and counts must be non-negative")
        if self.archive_epoch < 0:
            raise ValueError("archive epoch must be non-negative")
        if sum(self.domain_counts.values()) != self.row_count:
            raise ValueError("archive domain counts must equal row count")
        if any(count < 0 for count in self.domain_counts.values()):
            raise ValueError("archive domain counts must be non-negative")

    def can_delete(
        self,
        *,
        state: ArchiveState,
        current_archive_epoch: int,
        replay_lease_active: bool,
    ) -> bool:
        self.validate()
        return (
            state is ArchiveState.DELETION_ELIGIBLE
            and current_archive_epoch == self.archive_epoch
            and not replay_lease_active
        )
