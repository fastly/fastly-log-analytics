"""Contracts for the future high-scale data plane.

This module deliberately does not enable a runtime deployment mode. It records
the ownership and safety boundary that a high-scale implementation must satisfy
before it can replace the existing standard or high-throughput paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

HIGH_SCALE_MODE = "high_scale"

HIGH_SCALE_OWNERSHIP = {
    "source_discovery": "high_scale_ledger",
    "durable_events": "fos_archive",
    "serving": "clickhouse",
    "source_deletion": "archive_deletion_controller",
    "rum": "high_scale_rum_pipeline",
    "cmcd": "request_event_projection",
}


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


@dataclass(frozen=True)
class MigrationFence:
    """Ownership handoff record shared by the current and replacement planes."""

    service_id: str
    owner_epoch: int
    current_owner: str
    next_owner: str
    source_cursor: str
    drain_complete: bool = False
    cutover_committed: bool = False
    rollback_allowed: bool = False

    def can_cut_over(self) -> bool:
        return self.drain_complete and self.owner_epoch > 0 and bool(self.source_cursor)

    def can_roll_back(self) -> bool:
        return self.rollback_allowed and self.cutover_committed and self.drain_complete


@dataclass(frozen=True)
class RecoveryBudget:
    events_per_second: int
    recovery_window_seconds: int
    archive_read_events_per_second: int
    decode_events_per_second: int
    clickhouse_insert_events_per_second: int
    replication_factor: int
    live_ingest_reservation: float

    def __post_init__(self) -> None:
        if self.events_per_second <= 0 or self.recovery_window_seconds <= 0:
            raise ValueError("event rate and recovery window must be positive")
        if (
            min(
                self.archive_read_events_per_second,
                self.decode_events_per_second,
                self.clickhouse_insert_events_per_second,
            )
            <= 0
        ):
            raise ValueError("recovery throughput values must be positive")
        if self.replication_factor < 1:
            raise ValueError("replication factor must be at least 1")
        if not 0 <= self.live_ingest_reservation < 1:
            raise ValueError("live ingest reservation must be in [0, 1)")

    @property
    def replay_events_per_second(self) -> float:
        return self.events_per_second * 86_400 / self.recovery_window_seconds

    @property
    def required_replay_events_per_second(self) -> float:
        return self.replay_events_per_second / (1 - self.live_ingest_reservation)

    @property
    def can_recover(self) -> bool:
        return (
            min(
                self.archive_read_events_per_second,
                self.decode_events_per_second,
                self.clickhouse_insert_events_per_second,
            )
            >= self.required_replay_events_per_second
        )
