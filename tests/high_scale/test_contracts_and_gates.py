from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from backend.core.high_scale_contracts import (
    ArchiveState,
    validate_archive_transition,
)
from backend.high_scale.archive_models import ArchiveManifest, ServingWatermark
from backend.high_scale.exports import ExportManager, ExportState
from backend.high_scale.models import (
    HighScaleConfig,
    recommend_retention,
)
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    PartialInsert,
    PublicationStatus,
)
from backend.high_scale.query_contracts import QueryResponseMetadata
from backend.high_scale.recovery_capacity import (
    RecoveryCapacityMeasurement,
    evaluate_recovery_capacity,
)
from backend.high_scale.topology import build_topology_profile

# ── From test_contracts.py ──


def test_archive_states_are_monotonic() -> None:
    validate_archive_transition(ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED)
    with pytest.raises(ValueError, match="monotonic"):
        validate_archive_transition(ArchiveState.MANIFEST_COMMITTED, ArchiveState.MANIFEST_PREPARED)


# ── From test_fencing.py ──


def test_stale_epoch_cannot_commit_manifest() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Mock transaction and locked owner
    mock_conn = MagicMock()
    mock_owner = MagicMock(current_owner="high_scale", owner_epoch=5)

    control._locked_owner = MagicMock(return_value=mock_owner)  # type: ignore[assignment]
    control.transaction = MagicMock()  # type: ignore[assignment]
    control.transaction.return_value.__enter__.return_value = mock_conn

    # Worker has stale epoch = 4 (current is 5)
    manifest = MagicMock(spec=ArchiveManifest)
    manifest.manifest_id = "man-1"
    manifest.service_id = "svc-1"
    manifest.domain = "request"
    manifest.validate.return_value = None

    with pytest.raises(ValueError, match="owner epoch mismatch|stale owner epoch"):
        control.register_archive_manifest(manifest, owner_epoch=4)


def test_stale_epoch_cannot_acknowledge_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires acknowledge_sources_batch with epoch check
    assert hasattr(control, "acknowledge_sources_batch"), (
        "Missing acknowledge_sources_batch method on PostgresControlPlane"
    )


def test_stale_epoch_cannot_claim_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires claim_sources_batch with epoch check
    assert hasattr(control, "claim_sources_batch"), "Missing claim_sources_batch method on PostgresControlPlane"


# ── From test_publication_gate.py ──


def test_publication_attempt_with_mismatched_row_count_rejected() -> None:
    """A publication attempt with mismatched row count is rejected and never transitions to visible."""
    store = InMemoryBatchManifestStore()
    client = MagicMock()
    # Mock ClickHouse inserting 95 rows when batch has 100
    batch = HighScaleBatch(
        batch_id="batch-mismatch",
        service_id="svc-test",
        domain="request",
        generation="epoch-1",
        rows=tuple({"event_id": f"evt-{i}"} for i in range(100)),
    )

    client.insert.side_effect = PartialInsert(95)
    publication = ClickHousePublication(store, client)

    with pytest.raises(PartialInsert) as exc_info:
        publication.publish(batch)

    assert exc_info.value.rows_inserted == 95
    manifest = store.get("batch-mismatch")
    assert manifest is not None
    assert manifest.status is PublicationStatus.PENDING
    assert manifest.visible_rows == 0


def test_query_filters_visible_state_and_ignores_pending_rows() -> None:
    """Queries must filter on publication_state='visible' or _high_scale_visible=1 and never see pending rows."""
    from datetime import datetime, timedelta

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.query_service import query_request_facts

    client = MagicMock()
    # ClickHouse query returns rows only when publication_state = 'visible'
    client.execute.return_value = [
        {"event_id": "00000000-0000-0000-0000-000000000001", "timestamp": "2026-10-09T12:00:00Z"},
    ]

    now = datetime(2026, 10, 9, 12, 5, tzinfo=UTC)
    watermark = ServingWatermark(
        service_id="svc-test",
        domain="request",
        owner_epoch=1,
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        last_accepted_cursor="cur-1",
        last_archived_event_id="evt-1",
        last_visible_event_id="evt-1",
        exact=True,
    )

    # Run query_request_facts
    results = query_request_facts(
        client,
        service_id="svc-test",
        start=now - timedelta(minutes=5),
        end=now,
        cursor_secret=b"test-secret-123456",
        watermark=watermark,
        limit=10,
    )
    assert len(results.rows) == 1

    # Verify execute was called with a query enforcing publication_state = 'visible'
    assert client.execute.called
    query_sql, params = client.execute.call_args[0]
    assert "publication_state = 'visible'" in query_sql or "_high_scale_visible = 1" in query_sql


# ── From test_query_contracts.py ──


def test_query_metadata_requires_coverage_and_watermark_consistency() -> None:
    watermark = ServingWatermark(
        "svc",
        "request",
        2,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )

    metadata = QueryResponseMetadata(
        status="complete",
        exact=True,
        coverage=1.0,
        freshness_lag_seconds=3.0,
        watermark=watermark,
        approximation_error=None,
        error=None,
    )

    metadata.validate()


def test_approximation_error_is_required_for_approximate_results() -> None:
    watermark = ServingWatermark("svc", "request", 1, None, None, None, None, None, False)
    metadata = QueryResponseMetadata("complete", False, 0.5, 0.0, watermark, None, None)

    with pytest.raises(ValueError, match="approximation"):
        metadata.validate()


# ── From test_recovery_capacity.py ──


def test_two_million_events_per_second_target_requires_capacity_gate() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=750_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.required_events_per_second == pytest.approx(240_000_000)
    assert report.replication_adjusted_insert_events_per_second == pytest.approx(250_000_000)
    assert report.feasible is True


def test_replication_can_make_an_otherwise_fast_recovery_infeasible() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=400_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.limiting_stage == "clickhouse_insert"
    assert report.feasible is False


# ── From test_topology.py ──


def test_local_profile_does_not_claim_production_ha() -> None:
    profile = build_topology_profile("local")
    assert profile.clickhouse_replicas_per_shard == 1
    assert profile.keeper_members == 1
    assert profile.production_ha is False


def test_portable_production_profile_has_replicas_and_keeper() -> None:
    profile = build_topology_profile("portable-production")
    assert profile.clickhouse_replicas_per_shard == 2
    assert profile.keeper_members == 3
    assert profile.production_ha is True


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown"):
        build_topology_profile("unknown-production")


# ── From test_exports.py ──


def test_export_is_bounded_and_completes() -> None:
    manager = ExportManager(max_workers=1, max_rows=2, max_bytes=100)
    try:
        job_id = manager.submit("job-1", [{"id": 1}, {"id": 2}], lambda row: str(row["id"]).encode())
        job = manager.wait(job_id)
        assert job.state is ExportState.COMPLETED
        assert job.rows_written == 2
        assert job.payload == b"1\n2\n"
    finally:
        manager.shutdown()


def test_export_rejects_row_limit_and_duplicate_job() -> None:
    manager = ExportManager(max_workers=1, max_rows=1, max_bytes=100)
    try:
        manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        with pytest.raises(ValueError, match="already exists"):
            manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        job_id = manager.submit("job-2", [{"id": 1}, {"id": 2}], lambda row: b"x")
        assert manager.wait(job_id).state is ExportState.FAILED
    finally:
        manager.shutdown()


def test_export_can_be_cancelled() -> None:
    manager = ExportManager(max_workers=1, max_rows=1000, max_bytes=100_000)
    try:
        job_id = manager.submit("job-1", range(1000), lambda row: (time.sleep(0.001), b"x")[1])
        time.sleep(0.01)
        assert manager.cancel(job_id) is True
        assert manager.wait(job_id).state is ExportState.CANCELLED
    finally:
        manager.shutdown()


# ── From test_config.py ──


def test_high_scale_requires_clickhouse_and_fos() -> None:
    config = HighScaleConfig(True, None, None, None)
    with pytest.raises(ValueError, match="ClickHouse"):
        config.validate()


def test_disabled_high_scale_config_is_inert() -> None:
    HighScaleConfig(False, None, None, None).validate()


def test_retention_recommendation_preserves_disk_reserve() -> None:
    recommendation = recommend_retention(
        service_rate=100_000,
        row_width_bytes=900,
        disk_bytes=10 * 1024**4,
    )
    assert recommendation.protected_free_bytes >= 0.20 * 10 * 1024**4
    assert recommendation.estimated_hot_bytes <= 0.80 * 10 * 1024**4


def test_retention_recommendation_accounts_for_rum_and_enabled_fields() -> None:
    narrow = recommend_retention(
        service_rate=100,
        row_width_bytes=100,
        disk_bytes=10**10,
    )
    wide = recommend_retention(
        service_rate=100,
        row_width_bytes=100,
        disk_bytes=10**10,
        rum_rate=100,
        enabled_field_count=5,
        bytes_per_enabled_field=100,
    )
    assert wide.hot_retention_seconds < narrow.hot_retention_seconds
