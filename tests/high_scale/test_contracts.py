import pytest

from backend.core.high_scale_contracts import (
    ARCHIVE_STATES,
    HIGH_SCALE_MODE,
    HIGH_SCALE_OWNERSHIP,
    ArchiveManifest,
    ArchiveState,
    MigrationFence,
    RecoveryBudget,
    validate_archive_transition,
)


def test_high_scale_ownership_has_one_owner_per_boundary() -> None:
    assert HIGH_SCALE_MODE == "high_scale"
    assert HIGH_SCALE_OWNERSHIP == {
        "source_discovery": "high_scale_ledger",
        "durable_events": "fos_archive",
        "serving": "clickhouse",
        "source_deletion": "archive_deletion_controller",
        "rum": "high_scale_rum_pipeline",
        "cmcd": "request_event_projection",
    }


def test_archive_states_are_monotonic_and_manifest_fields_are_required() -> None:
    assert ARCHIVE_STATES == (
        ArchiveState.ARTIFACT_UPLOADING,
        ArchiveState.ARTIFACT_VERIFIED,
        ArchiveState.MANIFEST_PREPARED,
        ArchiveState.MANIFEST_COMMITTED,
        ArchiveState.DELETION_ELIGIBLE,
        ArchiveState.SOURCE_DELETED,
    )
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


def test_recovery_budget_covers_live_reservation_and_replay_rate() -> None:
    budget = RecoveryBudget(
        events_per_second=2_000_000,
        recovery_window_seconds=900,
        archive_read_events_per_second=300_000_000,
        decode_events_per_second=300_000_000,
        clickhouse_insert_events_per_second=300_000_000,
        replication_factor=3,
        live_ingest_reservation=0.25,
    )
    assert budget.replay_events_per_second == pytest.approx(192_000_000)
    assert budget.required_replay_events_per_second == pytest.approx(256_000_000)
    assert budget.can_recover is True

    with pytest.raises(ValueError, match="replication"):
        RecoveryBudget(
            events_per_second=1,
            recovery_window_seconds=1,
            archive_read_events_per_second=1,
            decode_events_per_second=1,
            clickhouse_insert_events_per_second=1,
            replication_factor=0,
            live_ingest_reservation=0.1,
        )


def test_migration_fence_requires_drain_before_cutover_and_rollback() -> None:
    fence = MigrationFence(
        service_id="service",
        owner_epoch=7,
        current_owner="high_throughput",
        next_owner="high_scale",
        source_cursor="raw/request/file.gz#42",
        drain_complete=True,
        cutover_committed=True,
        rollback_allowed=True,
    )
    assert fence.can_cut_over() is True
    assert fence.can_roll_back() is True

    undrained = MigrationFence(
        service_id="service",
        owner_epoch=7,
        current_owner="high_throughput",
        next_owner="high_scale",
        source_cursor="raw/request/file.gz#42",
        cutover_committed=True,
        rollback_allowed=True,
    )
    assert undrained.can_cut_over() is False
    assert undrained.can_roll_back() is False
